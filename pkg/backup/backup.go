package backup

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strconv"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-agent/pkg/storage"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"

	"github.com/vesoft-inc/nebula-br/pkg/clients"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
)

type Backup struct {
	ctx  context.Context
	cfg  *config.BackupConfig
	meta *clients.NebulaMeta

	hosts *utils.NebulaHosts
	sto   storage.ExternalStorage
}

func NewBackup(ctx context.Context, cfg *config.BackupConfig) (*Backup, error) {
	b := &Backup{
		ctx: context.WithValue(ctx, storage.SessionKey, uuid.NewString()),
		cfg: cfg,
	}

	var err error
	b.meta, err = clients.NewMeta(cfg.MetaAddr)
	if err != nil {
		return nil, fmt.Errorf("create meta client failed: %w", err)
	}

	b.sto, err = storage.New(cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("create storage failed: %w", err)
	}

	listRes, err := b.meta.ListCluster()
	if err != nil {
		return nil, fmt.Errorf("list cluster failed: %w", err)
	}
	b.hosts = &utils.NebulaHosts{}
	err = b.hosts.LoadFrom(listRes)
	if err != nil {
		return nil, fmt.Errorf("parse cluster response failed: %w", err)
	}
	return b, nil
}

// upload the meta backup files in host to external uri
// localDir are absolute meta checkpoint folder in host filesystem
// targetUri is external storage's uri, which is meta's root dir,
// has pattern like local://xxx, s3://xxx
func (b *Backup) uploadMeta(host *nebula.HostAddr, targetUri string, localDir string) error {
	agentAddr, err := b.hosts.GetAgentFor(b.meta.LeaderAddr())
	if err != nil {
		return err
	}
	agent, err := clients.NewAgent(b.ctx, agentAddr)
	if err != nil {
		return fmt.Errorf("create agent failed: %w", err)
	}

	backend, err := b.sto.GetDir(b.ctx, targetUri)
	if err != nil {
		return fmt.Errorf("get storage backend for %s failed: %w", targetUri, err)
	}
	req := &pb.UploadFileRequest{
		SourcePath:    localDir,
		TargetBackend: backend,
		Recursively:   true,
	}
	_, err = agent.UploadFile(req)
	if err != nil {
		return fmt.Errorf("upload file by agent failed: %w", err)
	}
	return nil
}

func (b *Backup) uploadStorage(hostDirs map[string]map[string][]string, targetUri string) error {
	for addrStr, spaceDirs := range hostDirs {
		// get storage node's agent
		addr, err := utils.ParseAddr(addrStr)
		if err != nil {
			return err
		}
		agentAddr, err := b.hosts.GetAgentFor(addr)
		if err != nil {
			return err
		}
		agent, err := clients.NewAgent(b.ctx, agentAddr)
		if err != nil {
			return err
		}

		logger := log.WithField("host", addrStr)
		// upload every space in this node
		for idStr, dirs := range spaceDirs {
			for i, source := range dirs {
				// {backupRoot}/{backupName}/data/{addr}/data{0..n}/{spaceId}
				target, _ := utils.UriJoin(targetUri, addrStr, fmt.Sprintf("data%d", i), idStr)
				backend, err := b.sto.GetDir(b.ctx, target)
				if err != nil {
					return fmt.Errorf("get storage backend for %s failed: %w", target, err)
				}

				req := &pb.UploadFileRequest{
					SourcePath:    source,
					TargetBackend: backend,
					Recursively:   true,
				}
				_, err = agent.UploadFile(req)
				if err != nil {
					return fmt.Errorf("upload %s to %s failed:%w", source, target, err)
				}
				logger.WithField("src", source).WithField("target", target).Info("Upload storage checkpoint successfully")
			}
		}
	}

	return nil
}

func (b *Backup) generateMetaFile(meta *meta.BackupMeta) (string, error) {
	tmpMetaPath := filepath.Join(utils.LocalTmpDir, fmt.Sprintf("%s.meta", string(meta.BackupName)))

	var fileNames [][]byte
	for _, pathBytes := range meta.MetaFiles {
		name := filepath.Base(string(pathBytes[:]))
		fileNames = append(fileNames, []byte(name))
	}
	meta.MetaFiles = fileNames

	return tmpMetaPath, utils.DumpMetaToFile(meta, tmpMetaPath)
}

// Backup backs up data in given external storage, and return the backup name
func (b *Backup) Backup() (string, error) {
	// call the meta service, create backup files in each local
	backupRes, err := b.meta.CreateBackup(b.cfg.Spaces)
	if err != nil {
		if backupRes != nil && backupRes.GetMeta() != nil && backupRes.GetMeta().GetBackupName() != nil {
			return string(backupRes.GetMeta().GetBackupName()), nil
		}
		return "", err
	}
	backupInfo := backupRes.GetMeta()
	backupName := string(backupInfo.GetBackupName())
	logger := log.WithField("name", backupName)
	logger.WithField("backup info", utils.StringifyBackup(backupInfo)).Info("Create backup in nebula machine's local")

	// ensure root dir
	rootUri, err := utils.UriJoin(b.cfg.Backend.Uri(), string(backupInfo.BackupName))
	if err != nil {
		return backupName, err
	}
	err = b.sto.EnsureDir(b.ctx, rootUri, false)
	if err != nil {
		return backupName, fmt.Errorf("ensure dir %s failed: %w", rootUri, err)
	}
	logger.WithField("root", rootUri).Info("Ensure backup root dir")

	// upload meta files
	metaDir, err := utils.UriJoin(rootUri, "meta")
	if err != nil {
		return backupName, err
	}
	if len(backupInfo.GetMetaFiles()) == 0 {
		return backupName, fmt.Errorf("there is no meta files in backup info")
	}
	localMetaDir := path.Dir(string(backupInfo.MetaFiles[0]))
	if err = b.uploadMeta(b.meta.LeaderAddr(), metaDir, localMetaDir); err != nil {
		return backupName, err
	}
	logger.WithField("meta", metaDir).Info("Upload meta successfully")

	// upload storage files
	storageDir, _ := utils.UriJoin(rootUri, "data")
	hostDirs := make(map[string]map[string][]string)
	// group checkpoint dirs by host and space id
	for sid, sb := range backupInfo.GetSpaceBackups() {
		idStr := strconv.FormatInt(int64(sid), 10)
		for _, hb := range sb.GetHostBackups() {
			hostStr := utils.StringifyAddr(hb.GetHost())
			for _, cp := range hb.GetCheckpoints() {
				if _, ok := hostDirs[hostStr]; !ok {
					hostDirs[hostStr] = make(map[string][]string)
				}

				hostDirs[hostStr][idStr] = append(hostDirs[hostStr][idStr], string(cp.GetPath()))
			}
		}
	}
	err = b.uploadStorage(hostDirs, storageDir)
	if err != nil {
		return backupName, fmt.Errorf("upload stoarge failed %w", err)
	}
	logger.WithField("data", storageDir).Info("Upload data backup successfully")

	// generate backup meta files and upload
	if err := utils.EnsureDir(utils.LocalTmpDir); err != nil {
		return backupName, err
	}
	defer func() {
		if err := utils.RemoveDir(utils.LocalTmpDir); err != nil {
			log.WithError(err).Errorf("Remove tmp dir %s failed", utils.LocalTmpDir)
		}
	}()

	tmpMetaPath, err := b.generateMetaFile(backupInfo)
	if err != nil {
		return backupName, fmt.Errorf("write meta to tmp path failed: %w", err)
	}
	logger.WithField("tmp path", tmpMetaPath).Info("Write meta data to local tmp file successfully")
	backupMetaPath, err := utils.UriJoin(rootUri, filepath.Base(tmpMetaPath))
	if err != nil {
		return backupName, err
	}
	err = b.sto.Upload(b.ctx, backupMetaPath, tmpMetaPath, false)
	if err != nil {
		return backupName, fmt.Errorf("upload local tmp file to remote storage %s failed: %w", backupMetaPath, err)
	}
	logger.WithField("remote path", backupMetaPath).Info("Upload tmp backup meta file to remote")

	// drop backup files in cluster machine local and local tmp files
	err = b.meta.DropBackup(backupInfo.GetBackupName())
	if err != nil {
		return backupName, fmt.Errorf("drop backup %s in cluster local failed: %w",
			string(backupInfo.BackupName[:]), err)
	}
	logger.Info("Drop backup in cluster and local tmp folder successfully")

	return backupName, nil
}
