package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	_ "os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/vesoft-inc/nebula-br/pkg/config"
	backupCtx "github.com/vesoft-inc/nebula-br/pkg/context"
	"github.com/vesoft-inc/nebula-br/pkg/metaclient"
	"github.com/vesoft-inc/nebula-br/pkg/remote"
	"github.com/vesoft-inc/nebula-br/pkg/storage"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

var defaultTimeout time.Duration = 120 * time.Second
var tmpDir = "/tmp/"

type BackupError struct {
	msg string
	Err error
}

type spaceInfo struct {
	spaceID       nebula.GraphSpaceID
	checkpointDir string
}

var LeaderNotFoundError = errors.New("not found leader")
var backupFailed = errors.New("backup failed")

func (e *BackupError) Error() string {
	return e.msg + e.Err.Error()
}

type backupEntry struct {
	SrcPath string
	DestUrl string
}
type idPathMap map[string][]backupEntry
type Backup struct {
	config         config.BackupConfig
	metaLeader     string
	backendStorage storage.ExternalStorage
	log            *zap.Logger
	metaFileName   string
	storageMap     map[string]idPathMap
	metaMap        map[string]idPathMap
	storeCtx       *backupCtx.Context
}

func NewBackupClient(cf config.BackupConfig, log *zap.Logger) (*Backup, error) {
	local_addr, err := remote.GetAddresstoReachRemote(strings.Split(cf.Meta, ":")[0], cf.User, log)
	if err != nil {
		log.Error("get local address failed", zap.Error(err))
		return nil, err
	}
	log.Info("local address", zap.String("address", local_addr))
	var (
		storeCtx backupCtx.Context
		backend  storage.ExternalStorage
	)
	backend, err = storage.NewExternalStorage(cf.BackendUrl, log, cf.MaxConcurrent, cf.CommandArgs,
		&storeCtx)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil, err
	}

	b := &Backup{config: cf, log: log,
		storageMap: make(map[string]idPathMap),
		metaMap:    make(map[string]idPathMap),
		storeCtx:   &storeCtx}

	b.storeCtx.LocalAddr = local_addr
	b.storeCtx.Reporter = b
	b.backendStorage = backend

	return b, nil
}

func (b *Backup) dropBackup(name []byte) (*meta.ExecResp, error) {

	client := metaclient.NewMetaClient(b.log)
	err := client.Open(b.metaLeader)
	if err != nil {
		return nil, err
	}

	snapshot := meta.NewDropSnapshotReq()
	snapshot.Name = name
	defer client.Close()

	resp, err := client.DropBackup(snapshot)
	if err != nil {
		return nil, err
	}

	if resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
		return nil, fmt.Errorf("drop backup failed %d", resp.GetCode())
	}

	return resp, nil
}

func (b *Backup) createBackup() (*meta.CreateBackupResp, error) {
	b.metaLeader = b.config.Meta

	for {
		client := metaclient.NewMetaClient(b.log)
		err := client.Open(b.metaLeader)
		if err != nil {
			return nil, err
		}

		backupReq := meta.NewCreateBackupReq()
		defer client.Close()
		if len(b.config.SpaceNames) != 0 {
			for _, name := range b.config.SpaceNames {
				backupReq.Spaces = append(backupReq.Spaces, []byte(name))
			}
		}

		resp, err := client.CreateBackup(backupReq)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() != nebula.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
			b.log.Error("backup failed", zap.String("error code", resp.GetCode().String()))
			return nil, backupFailed
		}

		if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
			return resp, nil
		}

		leader := resp.GetLeader()
		if leader == meta.ExecResp_Leader_DEFAULT {
			return nil, LeaderNotFoundError
		}

		b.log.Info("leader changed", zap.String("leader", leader.String()))
		b.metaLeader = metaclient.HostaddrToString(leader)
	}
}

func (b *Backup) writeMetadata(meta *meta.BackupMeta) error {
	b.metaFileName = tmpDir + string(meta.BackupName[:]) + ".meta"

	var absMetaFiles [][]byte
	for _, files := range meta.MetaFiles {
		f := filepath.Base(string(files[:]))
		absMetaFiles = append(absMetaFiles, []byte(f))
	}
	meta.MetaFiles = absMetaFiles

	return utils.PutMetaToFile(b.log, meta, b.metaFileName)
}

func (b *Backup) BackupCluster() error {
	b.log.Info("start backup nebula cluster")
	resp, err := b.createBackup()
	if err != nil {
		b.log.Error("backup cluster failed", zap.Error(err))
		return err
	}

	meta := resp.GetMeta()
	b.log.Info("response backup meta",
		zap.String("backup.meta", metaclient.BackupMetaToString(meta)))

	err = b.uploadAll(meta)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backup) execPreUploadMetaCommand(metaDir string) error {
	cmdStr := []string{"mkdir", "-p", metaDir}
	b.log.Info("exec pre upload meta command", zap.Strings("cmd", cmdStr))
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()
	return nil
}

func (b *Backup) uploadMeta(g *errgroup.Group, files []string) {

	b.log.Info("start upload meta", zap.String("addr", b.metaLeader))
	ipAddr := strings.Split(b.metaLeader, ":")
	b.storeCtx.RemoteAddr = ipAddr[0]

	b.log.Info("will upload meta", zap.Int("sst file count", len(files)))
	cmd := b.backendStorage.BackupMetaCommand(files)

	func(addr string, user string, cmd string, log *zap.Logger) {
		g.Go(func() error {
			client, err := remote.NewClient(addr, user, log)
			if err != nil {
				return err
			}
			defer client.Close()
			return client.ExecCommandBySSH(cmd)
		})
	}(ipAddr[0], b.config.User, cmd, b.log)
}

func (b *Backup) uploadStorage(g *errgroup.Group, dirs map[string][]spaceInfo) error {
	b.log.Info("uploadStorage", zap.Int("dirs length", len(dirs)))
	for k, v := range dirs {
		b.log.Info("start upload storage", zap.String("addr", k))
		idMap := make(map[string][]string)
		for _, info := range v {
			idStr := strconv.FormatInt(int64(info.spaceID), 10)
			idMap[idStr] = append(idMap[idStr], info.checkpointDir)
		}

		ipAddrs := strings.Split(k, ":")
		b.log.Info("uploadStorage idMap", zap.Int("idMap length", len(idMap)))
		clients, err := remote.NewClientPool(ipAddrs[0], b.config.User, b.log, b.config.MaxSSHConnections)
		if err != nil {
			b.log.Error("new clients failed", zap.Error(err))
			return err
		}
		i := 0

		b.storeCtx.RemoteAddr = ipAddrs[0]

		//We need to limit the number of ssh connections per storage node
		for id2, cp := range idMap {
			cmds := b.backendStorage.BackupStorageCommand(cp, k, id2)
			for _, cmd := range cmds {
				if i >= len(clients) {
					i = 0
				}
				client := clients[i]
				func(client *remote.Client, cmd string) {
					g.Go(func() error {
						return client.ExecCommandBySSH(cmd)
					})
				}(client, cmd)
			}
			i++
		}
	}
	return nil
}

func (b *Backup) uploadMetaFile() error {
	cmdStr := b.backendStorage.BackupMetaFileCommand(b.metaFileName)
	b.log.Info("will upload metafile", zap.Strings("cmd", cmdStr))

	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (b *Backup) execPreCommand(backupName string) error {
	b.backendStorage.SetBackupName(backupName)
	cmdStr := b.backendStorage.BackupPreCommand()
	if cmdStr == nil {
		return nil
	}
	b.log.Info("exec pre command", zap.Strings("cmd", cmdStr))
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (b *Backup) uploadAll(meta *meta.BackupMeta) error {
	//upload meta
	g, _ := errgroup.WithContext(context.Background())

	err := b.execPreCommand(string(meta.GetBackupName()[:]))
	if err != nil {
		b.log.Error("exec pre command failed", zap.Error(err))
		return err
	}

	if b.backendStorage.Scheme() == storage.SCHEME_LOCAL { // NB: only local backend need this
		err = b.execPreUploadMetaCommand(b.backendStorage.BackupMetaDir())
		if err != nil {
			b.log.Error("exec pre uploadmeta command failed", zap.Error(err))
			return err
		}
	}

	var metaFiles []string
	for _, f := range meta.GetMetaFiles() {
		fileName := string(f[:])
		metaFiles = append(metaFiles, string(fileName))
	}
	b.uploadMeta(g, metaFiles)
	//upload storage
	storageMap := make(map[string][]spaceInfo)
	for k, v := range meta.GetBackupInfo() {
		for _, i := range v.GetInfo() {
			for _, f := range i.GetInfo() {
				dir := string(f.Path)
				cpDir := spaceInfo{k, dir}
				storageMap[metaclient.HostaddrToString(i.Host)] = append(storageMap[metaclient.HostaddrToString(i.Host)], cpDir)
			}
		}
	}

	err = b.uploadStorage(g, storageMap)
	if err != nil {
		return err
	}

	err = g.Wait()
	if err != nil {
		b.log.Error("upload error", zap.Error(err))
		return err
	}
	// write the meta for this backup to local

	err = b.writeMetadata(meta)
	if err != nil {
		b.log.Error("write the meta file failed", zap.Error(err))
		return err
	}
	b.log.Info("write meta data finished")
	// upload meta file
	err = b.uploadMetaFile()
	if err != nil {
		b.log.Error("upload meta file failed", zap.Error(err))
		return err
	}

	_, err = b.dropBackup(meta.GetBackupName())
	if err != nil {
		b.log.Error("drop backup failed", zap.Error(err))
	}

	b.log.Info("backup nebula cluster finished", zap.String("backupName", string(meta.GetBackupName()[:])))

	return nil
}

func (b *Backup) ShowSummaries() {
	fmt.Printf("==== backup summeries ====\n")
	fmt.Printf("localaddr       : %s\n", b.storeCtx.LocalAddr)
	fmt.Printf("backend.type    : %s\n", b.backendStorage.Scheme())
	fmt.Printf("backend.url     : %s\n", b.backendStorage.URI())
	fmt.Printf("tgt.meta.leader : %s\n", b.config.Meta)
	if b.backendStorage.Scheme() == storage.SCHEME_LOCAL {
		// if local, storages' snapshot would be copy to a path at that host.
		b.showUploadSummaries(&b.metaMap, "tgt.meta.map")
		b.showUploadSummaries(&b.storageMap, "tgt.storage.map")
	}
	fmt.Printf("==========================\n")
}

func (b *Backup) showUploadSummaries(m *map[string]idPathMap, msg string) {
	o, _ := json.MarshalIndent(m, "", "  ")
	fmt.Printf("--- %s ---\n", msg)
	fmt.Printf("%s\n", string(o))
}

func (b *Backup) doRecordUploading(m *map[string]idPathMap, spaceId string, host string, paths []string, desturl string) {
	if (*m)[host] == nil {
		(*m)[host] = make(idPathMap)
	}
	bes := []backupEntry{}
	for _, p := range paths {
		bes = append(bes, backupEntry{SrcPath: p, DestUrl: desturl})
	}
	(*m)[host][spaceId] = append((*m)[host][spaceId], bes[:]...)
}

func (b *Backup) StorageUploadingReport(spaceid string, host string, paths []string, desturl string) {
	b.doRecordUploading(&b.storageMap, spaceid, host, paths, desturl)
}

func (b *Backup) MetaUploadingReport(host string, paths []string, desturl string) {
	kDefaultSid := "0"
	b.doRecordUploading(&b.metaMap, kDefaultSid, host, paths, desturl)
}
