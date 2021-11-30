package restore

import (
	"context"
	"fmt"
	_ "os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vesoft-inc/nebula-agent/pkg/storage"
	"github.com/vesoft-inc/nebula-br/pkg/clients"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/utils"

	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

func GetBackupSuffix() string {
	return fmt.Sprintf("_old_%d", time.Now().Unix())
}

type Restore struct {
	ctx      context.Context
	cfg      *config.RestoreConfig
	sto      storage.ExternalStorage
	hosts    *utils.NebulaHosts
	meta     *clients.NebulaMeta
	agentMgr *clients.AgentManager

	rootUri    string
	backupName string
	backSuffix string
}

func NewRestore(ctx context.Context, cfg *config.RestoreConfig) (*Restore, error) {
	sto, err := storage.New(cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("create storage failed: %w", err)
	}

	client, err := clients.NewMeta(cfg.MetaAddr)
	if err != nil {
		return nil, fmt.Errorf("create meta client failed: %w", err)
	}

	listRes, err := client.ListCluster()
	if err != nil {
		return nil, fmt.Errorf("list cluster failed: %w", err)
	}
	hosts := &utils.NebulaHosts{}
	err = hosts.LoadFrom(listRes)
	if err != nil {
		return nil, fmt.Errorf("parse cluster response failed: %w", err)
	}

	return &Restore{
		ctx:        ctx,
		cfg:        cfg,
		sto:        sto,
		meta:       client,
		hosts:      hosts,
		agentMgr:   clients.NewAgentManager(ctx, hosts),
		rootUri:    cfg.Backend.Uri(),
		backupName: cfg.BackupName,
	}, nil
}

func (r *Restore) checkPhysicalTopology(info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) error {
	var (
		backupPaths    = make(map[int]int)
		backupStorages = make(map[string]bool)
	)

	for _, space := range info {
		for _, host := range space.GetHostBackups() {
			if _, ok := backupStorages[utils.StringifyAddr(host.GetHost())]; !ok {
				pathCnt := len(host.GetCheckpoints())
				backupPaths[pathCnt]++
			}
			backupStorages[utils.StringifyAddr(host.GetHost())] = true
		}
	}
	if r.hosts.StorageCount() != len(backupStorages) {
		return fmt.Errorf("the physical topology of storage count must be consistent, cluster: %d, backup: %d",
			r.hosts.StorageCount(), len(backupStorages))
	}

	clusterPaths := r.hosts.StoragePaths()
	if !reflect.DeepEqual(backupPaths, clusterPaths) {
		log.WithField("backup", backupPaths).WithField("cluster", clusterPaths).Error("Path distribution is not consistent")
		return fmt.Errorf("the physical topology is not consistent, path distribution is not consistent")
	}

	return nil
}

func (r *Restore) checkAndDropSpaces(info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) error {
	for sid, backup := range info {
		resp, err := r.meta.GetSpace(backup.Space.SpaceName)
		if err != nil {
			return fmt.Errorf("get info of space %s failed: %w", string(backup.Space.SpaceName), err)
		}

		if resp.GetCode() == nebula.ErrorCode_E_SPACE_NOT_FOUND {
			continue
		}
		if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
			if resp.Item.SpaceID != sid {
				return fmt.Errorf("space to resotre already exist and the space id is not consistent, name: %s, backup: %d, cluster: %d",
					string(backup.Space.SpaceName), sid, resp.Item.SpaceID)
			}
		} else {
			return fmt.Errorf("get info of space %s failed: %s",
				string(backup.Space.SpaceName), resp.GetCode().String())
		}
	}

	for _, backup := range info {
		err := r.meta.DropSpace(backup.Space.SpaceName, true)
		if err != nil {
			return fmt.Errorf("drop space %s failed: %w", string(backup.Space.SpaceName), err)
		}
	}

	return nil
}

func (r *Restore) backupOriginal(allspaces bool) error {
	r.backSuffix = GetBackupSuffix()

	for _, s := range r.hosts.GetStorages() {
		agent, err := r.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for storaged %s failed: %w",
				utils.StringifyAddr(s.GetAddr()), err)
		}

		logger := log.WithField("addr", utils.StringifyAddr(s.GetAddr()))
		for _, d := range s.Dir.Data {
			opath := filepath.Join(string(d), "nebula")
			bpath := fmt.Sprintf("%s%s", opath, r.backSuffix)
			req := &pb.MoveDirRequest{
				SrcPath: opath,
				DstPath: bpath,
			}
			_, err = agent.MoveDir(req)
			if err != nil {
				return fmt.Errorf("move dir from %s to %s failed: %w", opath, bpath, err)
			}

			logger.WithField("origin path", opath).
				WithField("backup path", bpath).
				Info("Backup origin storage data path successfully")
		}
	}

	if allspaces {
		for _, m := range r.hosts.GetMetas() {
			agent, err := r.agentMgr.GetAgentFor(m.GetAddr())
			if err != nil {
				return fmt.Errorf("get agent for metad %s failed: %w",
					utils.StringifyAddr(m.GetAddr()), err)
			}

			if len(m.Dir.Data) != 1 {
				return fmt.Errorf("meta service: %s should only have one data dir, but %d",
					utils.StringifyAddr(m.GetAddr()), len(m.Dir.Data))
			}

			opath := fmt.Sprintf("%s/nebula", string(m.Dir.Data[0]))
			bpath := fmt.Sprintf("%s%s", opath, r.backSuffix)

			req := &pb.MoveDirRequest{
				SrcPath: opath,
				DstPath: bpath,
			}
			_, err = agent.MoveDir(req)
			if err != nil {
				return fmt.Errorf("move dir from %s to %s failed: %w", opath, bpath, err)
			}

			log.WithField("addr", utils.StringifyAddr(m.GetAddr())).
				WithField("origin path", opath).
				WithField("backup path", bpath).
				Info("Backup origin meta data path successfully")
		}

	}
	return nil
}

func (r *Restore) downloadMeta() error {
	// {backupRoot}/{backupName}/meta/*.sst
	externalUri, _ := utils.UriJoin(r.rootUri, r.backupName, "meta")
	backend, err := r.sto.GetDir(r.ctx, externalUri)
	if err != nil {
		return fmt.Errorf("get storage backend for %s failed: %w", externalUri, err)
	}

	// download meta backup files to every meta service
	for _, s := range r.hosts.GetMetas() {
		agent, err := r.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for metad %s failed: %w",
				utils.StringifyAddr(s.GetAddr()), err)
		}

		// meta kv data path: {nebulaData}/meta
		localDir := string(s.Dir.Data[0])
		req := &pb.DownloadFileRequest{
			SourceBackend: backend,
			TargetPath:    localDir,
			Recursively:   true,
		}
		_, err = agent.DownloadFile(req)
		if err != nil {
			return fmt.Errorf("download meta files from %s to %s failed: %w", externalUri, localDir, err)
		}
	}

	return nil
}

func (r *Restore) downloadStorage(backup *meta.BackupMeta) (map[string]string, error) {
	// TODO(spw): only support same ip now, by sorting address
	// could match by label(or id) in the future, now suppose the label is ip.

	// current cluster storage serivce list
	currList := r.hosts.GetStorages()
	sort.Slice(currList, func(i, j int) bool {
		if currList[i].Addr.Host != currList[j].Addr.Host {
			return currList[i].Addr.Host < currList[j].Addr.Host
		}
		return currList[i].Addr.Port < currList[j].Addr.Port
	})

	// previous backup storage service list
	prevMap := make(map[string]*nebula.HostAddr)
	for _, sb := range backup.SpaceBackups {
		for _, hb := range sb.HostBackups {
			prevMap[utils.StringifyAddr(hb.GetHost())] = hb.GetHost()
		}
	}
	prevList := make([]*nebula.HostAddr, 0, len(prevMap))
	for _, addr := range prevMap {
		prevList = append(prevList, addr)
	}
	sort.Slice(prevList, func(i, j int) bool {
		if prevList[i].Host != prevList[j].Host {
			return prevList[i].Host < prevList[j].Host
		}
		return prevList[i].Port < prevList[j].Port
	})

	// download from previous to current one host by another
	serviceMap := make(map[string]string)
	// {backupRoot}/{backupName}/data/{addr}/data{0..n}/
	storageUri, _ := utils.UriJoin(r.rootUri, r.backupName, "data")
	for idx, s := range currList {
		agent, err := r.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return nil, fmt.Errorf("get agent for storaged %s failed: %w",
				utils.StringifyAddr(s.GetAddr()), err)
		}

		logger := log.WithField("addr", utils.StringifyAddr(s.GetAddr()))
		for i, d := range s.Dir.Data {
			// {backupRoot}/{backupName}/data/{addr}/data{0..n}/
			externalUri, _ := utils.UriJoin(storageUri, utils.StringifyAddr(prevList[idx]), fmt.Sprintf("data%d", i))
			backend, err := r.sto.GetDir(r.ctx, externalUri)
			if err != nil {
				return nil, fmt.Errorf("get storage backend for %s failed: %w", externalUri, err)
			}
			// {nebulaDataPath}/storage/nebula
			localDir := filepath.Join(string(d), "nebula")

			req := &pb.DownloadFileRequest{
				SourceBackend: backend,
				TargetPath:    localDir,
				Recursively:   true,
			}

			_, err = agent.DownloadFile(req)
			if err != nil {
				return nil, fmt.Errorf("download from %s to %s:%s failed: %w",
					externalUri, localDir, utils.StringifyAddr(s.GetAddr()), err)
			}
			logger.WithField("external", externalUri).
				WithField("local", localDir).Info("Download storage data successfully")
		}

		serviceMap[utils.StringifyAddr(prevList[idx])] = utils.StringifyAddr(s.GetAddr())
	}

	return serviceMap, nil
}

func (r *Restore) startMetaService() error {
	for _, meta := range r.hosts.GetMetas() {
		agent, err := r.agentMgr.GetAgentFor(meta.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for metad %s failed: %w",
				utils.StringifyAddr(meta.GetAddr()), err)
		}

		req := &pb.StartServiceRequest{
			Role: pb.ServiceRole_META,
			Dir:  string(meta.Dir.Root),
		}

		_, err = agent.StartService(req)
		if err != nil {
			return fmt.Errorf("start meta service %s by agent failed: %w",
				utils.StringifyAddr(meta.GetAddr()), err)
		}
		log.WithField("addr", utils.StringifyAddr(meta.GetAddr())).
			Info("Start meta service successfully")
	}

	return nil
}

func (r *Restore) stopCluster() error {
	rootDirs := r.hosts.GetRootDirs()
	for _, agentAddr := range r.hosts.GetAgents() {
		agent, err := r.agentMgr.GetAgent(agentAddr)
		if err != nil {
			return fmt.Errorf("get agent %s failed: %w", utils.StringifyAddr(agentAddr), err)
		}

		dirs, ok := rootDirs[agentAddr.Host]
		if !ok {
			log.WithField("host", agentAddr.Host).Info("Does not find nebula root dirs in this host")
			continue
		}

		logger := log.WithField("host", agentAddr.Host)
		for _, d := range dirs {
			req := &pb.StopServiceRequest{
				Role: pb.ServiceRole_ALL,
				Dir:  d.Dir,
			}
			logger.WithField("dir", d.Dir).Info("Stop services")
			_, err := agent.StopService(req)
			if err != nil {
				return fmt.Errorf("stop services in host %s failed: %w", agentAddr.Host, err)
			}
		}
	}
	return nil
}

func (r *Restore) restoreMeta(backup *meta.BackupMeta, storageMap map[string]string) error {
	addrMap := make([]*meta.HostPair, 0, len(storageMap))
	for from, to := range storageMap {
		fromaddr, err := utils.ParseAddr(from)
		if err != nil {
			return fmt.Errorf("parse %s failed: %w", from, err)
		}
		toaddr, err := utils.ParseAddr(to)
		if err != nil {
			return fmt.Errorf("parse %s failed: %w", to, err)
		}

		addrMap = append(addrMap, &meta.HostPair{FromHost: fromaddr, ToHost: toaddr})
	}

	for _, meta := range r.hosts.GetMetas() {
		metaSsts := make([]string, 0, len(backup.GetMetaFiles()))
		for _, f := range backup.GetMetaFiles() {
			// TODO(spw): data folder end with '/'?
			sstPath := fmt.Sprintf("%s/%s", string(meta.Dir.Data[0]), string(f))
			metaSsts = append(metaSsts, sstPath)
		}

		err := r.meta.RestoreMeta(meta.GetAddr(), addrMap, metaSsts)
		if err != nil {
			return fmt.Errorf("restore meta service %s failed: %w",
				utils.StringifyAddr(meta.GetAddr()), err)
		}

		log.WithField("addr", utils.StringifyAddr(meta.GetAddr())).
			Info("restore backup in this metad successfully")
	}

	return nil
}

func (r *Restore) startStorageService() error {
	for _, s := range r.hosts.GetStorages() {
		agent, err := r.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for storaged %s failed: %w",
				utils.StringifyAddr(s.GetAddr()), err)
		}

		req := &pb.StartServiceRequest{
			Role: pb.ServiceRole_STORAGE,
			Dir:  string(s.GetDir().GetRoot()),
		}
		_, err = agent.StartService(req)
		if err != nil {
			return fmt.Errorf("start storaged by agent failed: %w", err)
		}
		log.WithField("addr", utils.StringifyAddr(s.GetAddr())).
			Info("Start storaged by agent successfully")
	}

	return nil
}

func (r *Restore) startGraphService() error {
	for _, s := range r.hosts.GetGraphs() {
		agent, err := r.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for graphd %s failed: %w",
				utils.StringifyAddr(s.GetAddr()), err)
		}

		req := &pb.StartServiceRequest{
			Role: pb.ServiceRole_GRAPH,
			Dir:  string(s.GetDir().GetRoot()),
		}
		_, err = agent.StartService(req)
		if err != nil {
			return fmt.Errorf("start graphd by agent failed: %w", err)
		}
		log.WithField("addr", utils.StringifyAddr(s.GetAddr())).
			Info("Start graphd by agent successfully")
	}

	return nil
}

func (r *Restore) cleanupOriginalData() error {
	for _, m := range r.hosts.GetMetas() {
		agent, err := r.agentMgr.GetAgentFor(m.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for metad %s failed: %w",
				utils.StringifyAddr(m.GetAddr()), err)
		}

		req := &pb.RemoveDirRequest{
			Path: fmt.Sprintf("%s/nebula%s", string(m.Dir.Data[0]), r.backSuffix),
		}
		_, err = agent.RemoveDir(req)
		if err != nil {
			return fmt.Errorf("remove meta data dir %s by agent failed: %w", req.Path, err)
		}
		log.WithField("addr", utils.StringifyAddr(m.GetAddr())).
			WithField("path", req.Path).Info("Remove meta origin data successfully.")
	}

	for _, s := range r.hosts.GetStorages() {
		agent, err := r.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for storaged %s failed: %w",
				utils.StringifyAddr(s.GetAddr()), err)
		}

		logger := log.WithField("addr", utils.StringifyAddr(s.GetAddr()))
		for _, dir := range s.Dir.Data {
			req := &pb.RemoveDirRequest{
				Path: fmt.Sprintf("%s/nebula%s", string(dir), r.backSuffix),
			}
			_, err = agent.RemoveDir(req)
			if err != nil {
				return fmt.Errorf("remove storage data dir %s by agent failed: %w", req.Path, err)
			}
			logger.WithField("path", req.Path).Info("Remove storage origin data successfully.")
		}
	}
	return nil
}

// backup_root/backup_name
//   - meta
//      - xxx.sst
//      - ...
//   - data
//   - backup_name.meta
func (r *Restore) Restore() error {
	logger := log.WithField("backup", r.cfg.BackupName)
	// check backup dir existence
	rootUri, err := utils.UriJoin(r.cfg.Backend.Uri(), r.cfg.BackupName)
	if err != nil {
		return err
	}
	exist := r.sto.ExistDir(r.ctx, rootUri)
	if !exist {
		return fmt.Errorf("backup dir %s does not exist", rootUri)
	}
	logger.WithField("uri", rootUri).Info("Check backup dir successfully")

	// download and parse backup meta file
	if err := utils.EnsureDir(utils.LocalTmpDir); err != nil {
		return err
	}
	defer func() {
		if err := utils.RemoveDir(utils.LocalTmpDir); err != nil {
			log.WithError(err).Errorf("Remove tmp dir %s failed", utils.LocalTmpDir)
		}
	}()

	backupMetaName := fmt.Sprintf("%s.meta", r.cfg.BackupName)
	metaUri, _ := utils.UriJoin(rootUri, backupMetaName)
	tmpLocalPath := filepath.Join(utils.LocalTmpDir, backupMetaName)
	err = r.sto.Download(r.ctx, tmpLocalPath, metaUri, false)
	if err != nil {
		return fmt.Errorf("download %s to %s failed: %w", metaUri, tmpLocalPath, err)
	}
	bakMeta, err := utils.ParseMetaFromFile(tmpLocalPath)
	if err != nil {
		return fmt.Errorf("parse backup meta file %s failed: %w", tmpLocalPath, err)
	}

	// check this cluster's topology with info kept in backup meta
	err = r.checkPhysicalTopology(bakMeta.GetSpaceBackups())
	if err != nil {
		return fmt.Errorf("physical topology not consistent: %w", err)
	}

	// if only resotre some spaces, check and remove these spaces
	if !bakMeta.AllSpaces {
		err = r.checkAndDropSpaces(bakMeta.SpaceBackups)
		if err != nil {
			return fmt.Errorf("check and drop space failed: %w", err)
		}
		log.Info("Check and drop spaces successfully")
	}

	// stop cluster
	err = r.stopCluster()
	if err != nil {
		return fmt.Errorf("stop cluster failed: %w", err)
	}
	logger.Info("Stop cluster successfully")

	// backup original data if we are to restore whole cluster
	err = r.backupOriginal(bakMeta.AllSpaces)
	if err != nil {
		return fmt.Errorf("backup origin data path failed: %w", err)
	}
	logger.Info("Backup origin cluster data successfully")

	// download backup data from external storage to cluster
	err = r.downloadMeta()
	if err != nil {
		return fmt.Errorf("download meta data to cluster failed: %w", err)
	}
	log.Info("Download meta data to cluster successfully.")
	storageMap, err := r.downloadStorage(bakMeta)
	if err != nil {
		return fmt.Errorf("download storage data to cluster failed: %w", err)
	}
	log.Info("Download storage data to cluster successfully.")

	// start meta service first
	err = r.startMetaService()
	if err != nil {
		return fmt.Errorf("start meta service failed: %w", err)
	}
	time.Sleep(time.Second * 3)
	log.Info("Start meta service successfully.")

	// restore meta service by map
	err = r.restoreMeta(bakMeta, storageMap)
	if err != nil {
		return fmt.Errorf("restore cluster meta failed: %w", err)
	}
	log.Info("Restore meta service successfully.")

	// strat storage and graph service
	err = r.startStorageService()
	if err != nil {
		return fmt.Errorf("start storage service failed: %w", err)
	}
	err = r.startGraphService()
	if err != nil {
		return fmt.Errorf("start graph service failed: %w", err)
	}
	log.Info("Start storage and graph services successfully")

	// after success restore, cleanup the backup data if needed
	err = r.cleanupOriginalData()
	if err != nil {
		return fmt.Errorf("clean up origin data failed: %w", err)
	}
	log.Info("Cleanup origin data successfully")
	return nil
}
