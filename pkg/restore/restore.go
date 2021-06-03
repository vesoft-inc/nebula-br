package restore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/scylladb/go-set/strset"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/metaclient"
	"github.com/vesoft-inc/nebula-br/pkg/remote"
	"github.com/vesoft-inc/nebula-br/pkg/storage"

	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Restore struct {
	config       config.RestoreConfig
	backend      storage.ExternalStorage
	log          *zap.Logger
	metaLeader   string
	client       *metaclient.MetaClient
	storageNodes []config.NodeInfo
	metaNodes    []config.NodeInfo
	metaFileName string
}

type spaceInfo struct {
	spaceID nebula.GraphSpaceID
	cpDir   string
}

var LeaderNotFoundError = errors.New("not found leader")
var restoreFailed = errors.New("restore failed")
var listClusterFailed = errors.New("list cluster failed")
var spaceNotMatching = errors.New("Space mismatch")
var dropSpaceFailed = errors.New("drop space failed")
var getSpaceFailed = errors.New("get space failed")

func NewRestore(config config.RestoreConfig, log *zap.Logger) *Restore {
	backend, err := storage.NewExternalStorage(config.BackendUrl, log, config.MaxConcurrent, config.CommandArgs)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	backend.SetBackupName(config.BackupName)
	return &Restore{config: config, log: log, backend: backend}
}

func (r *Restore) checkPhysicalTopology(info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) error {
	s := strset.New()
	maxInfoLen := 0
	for _, v := range info {
		for _, i := range v.Info {
			s.Add(metaclient.HostaddrToString(i.Host))
			if len(i.Info) > maxInfoLen {
				maxInfoLen = len(i.Info)
			}
		}
	}

	if s.Size() > len(r.storageNodes) {
		return fmt.Errorf("The physical topology of storage must be consistent")
	}

	if maxInfoLen != len(r.storageNodes[0].DataDir) {
		return fmt.Errorf("The number of data directories for storage must be the same")
	}

	return nil
}

func (r *Restore) check() error {
	nodes := append(r.metaNodes, r.storageNodes...)
	command := r.backend.CheckCommand()
	return remote.CheckCommand(command, nodes, r.log)
}

func (r *Restore) downloadMetaFile() error {
	r.metaFileName = r.config.BackupName + ".meta"
	cmdStr := r.backend.RestoreMetaFileCommand(r.metaFileName, "/tmp/")
	r.log.Info("download metafile", zap.Strings("cmd", cmdStr))
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (r *Restore) restoreMetaFile() (*meta.BackupMeta, error) {
	file, err := os.OpenFile("/tmp/"+r.metaFileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryIn := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
	m := meta.NewBackupMeta()
	err = m.Read(binaryIn)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (r *Restore) downloadMeta(g *errgroup.Group, file []string) map[string][][]byte {
	sstFiles := make(map[string][][]byte)
	for _, n := range r.metaNodes {
		cmd, files := r.backend.RestoreMetaCommand(file, n.DataDir[0])
		ipAddr := strings.Split(n.Addrs, ":")
		func(addr string, user string, cmd string, log *zap.Logger) {
			g.Go(func() error {
				client, err := remote.NewClient(addr, user, log)
				if err != nil {
					return err
				}
				defer client.Close()
				return client.ExecCommandBySSH(cmd)
			})
		}(ipAddr[0], n.User, cmd, r.log)
		var byteFile [][]byte
		for _, f := range files {
			byteFile = append(byteFile, []byte(f))
		}
		sstFiles[n.Addrs] = byteFile
	}
	return sstFiles
}

func (r *Restore) downloadStorage(g *errgroup.Group, info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) map[string]string {
	idMap := make(map[string][]string)
	var cpHosts []string
	for gid, bInfo := range info {
		for _, i := range bInfo.Info {
			idStr := strconv.FormatInt(int64(gid), 10)
			if idMap[metaclient.HostaddrToString(i.Host)] == nil {
				cpHosts = append(cpHosts, metaclient.HostaddrToString(i.Host))
			}
			idMap[metaclient.HostaddrToString(i.Host)] = append(idMap[metaclient.HostaddrToString(i.Host)], idStr)
		}
	}

	sort.Strings(cpHosts)
	storageIPmap := make(map[string]string)
	for i, ip := range cpHosts {
		ids := idMap[ip]
		sNode := r.storageNodes[i]
		r.log.Info("download", zap.String("ip", ip), zap.String("storage", sNode.Addrs))
		var nebulaDirs []string
		for _, d := range sNode.DataDir {
			nebulaDirs = append(nebulaDirs, d+"/nebula")
		}

		cmds := r.backend.RestoreStorageCommand(ip, ids, nebulaDirs)
		addr := strings.Split(sNode.Addrs, ":")
		if ip != sNode.Addrs {
			storageIPmap[ip] = sNode.Addrs
		}
		for _, cmd := range cmds {
			func(addr string, user string, cmd string, log *zap.Logger) {
				g.Go(func() error {
					client, err := remote.NewClient(addr, user, log)
					if err != nil {
						return err
					}
					defer client.Close()
					return client.ExecCommandBySSH(cmd)
				})
			}(addr[0], sNode.User, cmd, r.log)
		}
	}
	return storageIPmap
}

func stringToHostAddr(host string) (*nebula.HostAddr, error) {
	ipAddr := strings.Split(host, ":")
	port, err := strconv.ParseInt(ipAddr[1], 10, 32)
	if err != nil {
		return nil, err
	}
	return &nebula.HostAddr{ipAddr[0], nebula.Port(port)}, nil
}

func sendRestoreMeta(addr string, files [][]byte, hostMap []*meta.HostPair, log *zap.Logger) error {

	// retry 3 times if restore failed
	count := 3
	for {
		if count == 0 {
			return restoreFailed
		}
		client := metaclient.NewMetaClient(log)
		err := client.Open(addr)
		if err != nil {
			log.Error("open meta failed", zap.Error(err), zap.String("addr", addr))
			time.Sleep(time.Second * 2)
			continue
		}
		defer client.Close()

		restoreReq := meta.NewRestoreMetaReq()
		restoreReq.Hosts = hostMap
		restoreReq.Files = files

		resp, err := client.RestoreMeta(restoreReq)
		if err != nil {
			// maybe we should retry
			log.Error("restore failed", zap.Error(err), zap.String("addr", addr))
			time.Sleep(time.Second * 2)
			count--
			continue
		}

		if resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
			log.Error("restore failed", zap.String("error code", resp.GetCode().String()), zap.String("addr", addr))
			time.Sleep(time.Second * 2)
			count--
			continue
		}
		log.Info("restore succeeded", zap.String("addr", addr))
		return nil
	}
}

func (r *Restore) restoreMeta(sstFiles map[string][][]byte, storageIDMap map[string]string) error {
	r.log.Info("restoreMeta")
	var hostMap []*meta.HostPair

	for k, v := range storageIDMap {
		fromAddr, err := stringToHostAddr(k)
		if err != nil {
			return err
		}
		toAddr, err := stringToHostAddr(v)
		if err != nil {
			return err
		}

		r.log.Info("restoreMeta host mapping", zap.String("fromAddr", fromAddr.String()), zap.String("toAddr", toAddr.String()))
		pair := &meta.HostPair{fromAddr, toAddr}
		hostMap = append(hostMap, pair)
	}
	r.log.Info("restoreMeta2", zap.Int("metaNode len:", len(r.metaNodes)))

	g, _ := errgroup.WithContext(context.Background())
	for _, n := range r.metaNodes {
		r.log.Info("will restore meta", zap.String("addr", n.Addrs))
		addr := n.Addrs
		func(addr string, files [][]byte, hostMap []*meta.HostPair, log *zap.Logger) {
			g.Go(func() error { return sendRestoreMeta(addr, files, hostMap, r.log) })
		}(addr, sstFiles[n.Addrs], hostMap, r.log)
	}
	r.log.Info("restoreMeta3")

	err := g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (r *Restore) cleanupOriginal() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.storageNodes {
		for _, d := range node.DataDir {
			cmd := r.backend.RestoreStoragePreCommand(d + "/nebula")
			ipAddr := strings.Split(node.Addrs, ":")[0]
			func(addr string, user string, cmd string, log *zap.Logger) {
				g.Go(func() error {
					client, err := remote.NewClient(addr, user, log)
					if err != nil {
						return err
					}
					defer client.Close()
					return client.ExecCommandBySSH(cmd)
				})
			}(ipAddr, node.User, cmd, r.log)
		}
	}

	for _, node := range r.metaNodes {
		for _, d := range node.DataDir {
			cmd := r.backend.RestoreMetaPreCommand(d + "/nebula")
			ipAddr := strings.Split(node.Addrs, ":")[0]
			func(addr string, user string, cmd string, log *zap.Logger) {
				g.Go(func() error {
					client, err := remote.NewClient(addr, user, log)
					if err != nil {
						return err
					}
					defer client.Close()
					return client.ExecCommandBySSH(cmd)
				})
			}(ipAddr, node.User, cmd, r.log)
		}
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) stopCluster() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.storageNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service stop storaged"
		ipAddr := strings.Split(node.Addrs, ":")[0]

		func(addr string, user string, cmd string, log *zap.Logger) {
			g.Go(func() error {
				client, err := remote.NewClient(addr, user, log)
				if err != nil {
					return err
				}
				defer client.Close()
				return client.ExecCommandBySSH(cmd)
			})
		}(ipAddr, node.User, cmd, r.log)
	}

	for _, node := range r.metaNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service stop metad"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		func(addr string, user string, cmd string, log *zap.Logger) {
			g.Go(func() error {
				client, err := remote.NewClient(addr, user, log)
				if err != nil {
					return err
				}
				defer client.Close()
				return client.ExecCommandBySSH(cmd)
			})
		}(ipAddr, node.User, cmd, r.log)
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) startMetaService() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.metaNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service start metad &>/dev/null &"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		func(addr string, user string, cmd string, log *zap.Logger) {
			g.Go(func() error {
				client, err := remote.NewClient(addr, user, log)
				if err != nil {
					return err
				}
				defer client.Close()
				return client.ExecCommandBySSH(cmd)
			})
		}(ipAddr, node.User, cmd, r.log)
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) startStorageService() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range r.storageNodes {
		cmd := "cd " + node.RootDir + " && scripts/nebula.service start storaged &>/dev/null &"
		ipAddr := strings.Split(node.Addrs, ":")[0]
		func(addr string, user string, cmd string, log *zap.Logger) {
			g.Go(func() error {
				client, err := remote.NewClient(addr, user, log)
				if err != nil {
					return err
				}
				defer client.Close()
				return client.ExecCommandBySSH(cmd)
			})
		}(ipAddr, node.User, cmd, r.log)
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) listCluster() (*meta.ListClusterInfoResp, error) {
	r.metaLeader = r.config.Meta

	for {
		client := metaclient.NewMetaClient(r.log)
		err := client.Open(r.metaLeader)
		if err != nil {
			return nil, err
		}

		listReq := meta.NewListClusterInfoReq()
		defer client.Close()

		resp, err := client.ListCluster(listReq)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() != nebula.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
			r.log.Error("list cluster failed", zap.String("error code", resp.GetCode().String()))
			return nil, listClusterFailed
		}

		if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
			return resp, nil
		}

		leader := resp.GetLeader()
		if leader == meta.ExecResp_Leader_DEFAULT {
			return nil, LeaderNotFoundError
		}

		r.log.Info("leader changed", zap.String("leader", leader.String()))
		r.metaLeader = metaclient.HostaddrToString(leader)
	}
}

func (r *Restore) getMetaInfo(hosts []*nebula.HostAddr) ([]config.NodeInfo, error) {

	var info []config.NodeInfo

	if len(hosts) == 0 {
		return nil, listClusterFailed
	}

	for _, v := range hosts {
		client := metaclient.NewMetaClient(r.log)
		addr := metaclient.HostaddrToString(v)
		r.log.Info("will get meta info", zap.String("addr", addr))
		err := client.Open(addr)
		if err != nil {
			return nil, err
		}

		dirReq := meta.NewGetMetaDirInfoReq()
		defer client.Close()

		resp, err := client.ListMetaDir(dirReq)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
			r.log.Error("list cluster failed", zap.String("error code", resp.GetCode().String()))
			return nil, listClusterFailed
		}
		var datadir []string

		for _, d := range resp.Dir.Data {
			datadir = append(datadir, string(d[0:]))
		}
		info = append(info, config.NodeInfo{Addrs: metaclient.HostaddrToString(v),
			User: r.config.User, RootDir: string(resp.Dir.Root[0:]), DataDir: datadir})
	}
	return info, nil
}

func (r *Restore) setStorageInfo(resp *meta.ListClusterInfoResp) {
	for _, v := range resp.StorageServers {
		var datadir []string

		for _, d := range v.Dir.Data {
			datadir = append(datadir, string(d[0:]))
		}

		r.storageNodes = append(r.storageNodes, config.NodeInfo{Addrs: metaclient.HostaddrToString(v.Host),
			User: r.config.User, RootDir: string(v.Dir.Root[0:]), DataDir: datadir})
	}

	sort.SliceStable(r.storageNodes, func(i, j int) bool {
		return r.storageNodes[i].Addrs < r.storageNodes[j].Addrs
	})
}

func (r *Restore) checkSpace(m *meta.BackupMeta) error {
	var client *metaclient.MetaClient
	reCreate := true

	for gid, info := range m.GetBackupInfo() {
		for {
			if reCreate {
				client = metaclient.NewMetaClient(r.log)
				err := client.Open(r.metaLeader)
				if err != nil {
					return err
				}
			}
			spaceReq := meta.NewGetSpaceReq()
			defer client.Close()
			spaceReq.SpaceName = info.Space.SpaceName

			resp, err := client.GetSpaceInfo(spaceReq)
			if err != nil {
				return err
			}

			if resp.GetCode() == nebula.ErrorCode_E_SPACE_NOT_FOUND {
				reCreate = false
				break
			}

			if resp.GetCode() != nebula.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
				r.log.Error("get space failed", zap.String("error code", resp.GetCode().String()))
				return getSpaceFailed
			}

			if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
				if resp.Item.SpaceID != gid {
					r.log.Error("space not matching", zap.String("spacename", string(info.Space.SpaceName[0:])),
						zap.Int32("gid", int32(gid)), zap.Int32("gid in server", int32(resp.Item.SpaceID)))
					return spaceNotMatching
				}
				reCreate = false
				break
			}

			leader := resp.GetLeader()
			if leader == meta.ExecResp_Leader_DEFAULT {
				return LeaderNotFoundError
			}

			r.log.Info("leader changed", zap.String("leader", leader.String()))
			r.metaLeader = metaclient.HostaddrToString(leader)
			reCreate = true
		}
	}
	return nil
}

func (r *Restore) dropSpace(m *meta.BackupMeta) error {

	var client *metaclient.MetaClient
	reCreate := true

	for _, info := range m.GetBackupInfo() {
		for {
			if reCreate {
				client = metaclient.NewMetaClient(r.log)
				err := client.Open(r.metaLeader)
				if err != nil {
					return err
				}
			}

			dropReq := meta.NewDropSpaceReq()
			defer client.Close()
			dropReq.SpaceName = info.Space.SpaceName
			dropReq.IfExists = true

			resp, err := client.DropSpace(dropReq)
			if err != nil {
				return err
			}

			if resp.GetCode() != nebula.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
				r.log.Error("drop space failed", zap.String("error code", resp.GetCode().String()))
				return dropSpaceFailed
			}

			if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
				reCreate = false
				break
			}

			leader := resp.GetLeader()
			if leader == meta.ExecResp_Leader_DEFAULT {
				return LeaderNotFoundError
			}

			r.log.Info("leader changed", zap.String("leader", leader.String()))
			r.metaLeader = metaclient.HostaddrToString(leader)
			reCreate = true
		}
	}
	return nil
}

func (r *Restore) RestoreCluster() error {

	resp, err := r.listCluster()
	if err != nil {
		r.log.Error("list cluster info failed", zap.Error(err))
		return err
	}

	r.setStorageInfo(resp)

	metaInfo, err := r.getMetaInfo(resp.GetMetaServers())
	if err != nil {
		return err
	}
	r.metaNodes = metaInfo

	for _, m := range metaInfo {
		r.log.Info("meta node", zap.String("node addr", m.Addrs))
	}

	err = r.check()

	if err != nil {
		r.log.Error("restore check failed", zap.Error(err))
		return err
	}

	err = r.downloadMetaFile()
	if err != nil {
		r.log.Error("download meta file failed", zap.Error(err))
		return err
	}

	m, err := r.restoreMetaFile()

	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	err = r.checkPhysicalTopology(m.BackupInfo)
	if err != nil {
		r.log.Error("check physical failed", zap.Error(err))
		return err
	}

	if !m.IncludeSystemSpace {
		err = r.checkSpace(m)
		if err != nil {
			r.log.Error("check space failed", zap.Error(err))
			return err
		}
		err = r.dropSpace(m)
		if err != nil {
			r.log.Error("drop space faile", zap.Error(err))
			return err
		}
	}

	err = r.stopCluster()
	if err != nil {
		r.log.Error("stop cluster failed", zap.Error(err))
		return err
	}

	if m.IncludeSystemSpace {
		err = r.cleanupOriginal()
		if err != nil {
			r.log.Error("cleanup original failed", zap.Error(err))
			return err
		}
	}

	g, _ := errgroup.WithContext(context.Background())

	var files []string

	for _, f := range m.MetaFiles {
		files = append(files, string(f[:]))
	}

	sstFiles := r.downloadMeta(g, files)
	storageIDMap := r.downloadStorage(g, m.BackupInfo)

	err = g.Wait()
	if err != nil {
		r.log.Error("restore error")
		return err
	}

	err = r.startMetaService()
	if err != nil {
		r.log.Error("start cluster failed", zap.Error(err))
		return err
	}

	time.Sleep(time.Second * 3)

	err = r.restoreMeta(sstFiles, storageIDMap)
	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	err = r.startStorageService()
	if err != nil {
		r.log.Error("start storage service failed", zap.Error(err))
		return err
	}

	r.log.Info("restore meta successed")

	return nil

}
