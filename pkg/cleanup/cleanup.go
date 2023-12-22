package cleanup

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-agent/pkg/storage"

	"github.com/vesoft-inc/nebula-br/pkg/clients"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
)

type Cleanup struct {
	ctx    context.Context
	cfg    *config.CleanupConfig
	client *clients.NebulaMeta
	sto    storage.ExternalStorage

	hosts    *utils.NebulaHosts
	agentMgr *clients.AgentManager
}

func NewCleanup(ctx context.Context, cfg *config.CleanupConfig) (*Cleanup, error) {
	sto, err := storage.New(cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("create storage for %s failed: %w", cfg.Backend.Uri(), err)
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

	return &Cleanup{
		ctx:      ctx,
		cfg:      cfg,
		client:   client,
		sto:      sto,
		hosts:    hosts,
		agentMgr: clients.NewAgentManager(ctx, hosts),
	}, nil
}

func (c *Cleanup) cleanNebula() error {
	err := c.client.DropBackup([]byte(c.cfg.BackupName))
	if err != nil {
		return fmt.Errorf("drop backup failed: %w", err)
	}
	log.Debugf("Drop backup %s successfully.", c.cfg.BackupName)

	return nil
}

func (c *Cleanup) cleanExternal() error {
	backupUri, err := utils.UriJoin(c.cfg.Backend.Uri(), c.cfg.BackupName)
	if err != nil {
		return err
	}

	err = c.sto.RemoveDir(c.ctx, backupUri)
	if err != nil {
		return fmt.Errorf("remove %s in external storage failed: %w", backupUri, err)
	}
	log.Debugf("Remove %s successfully.", backupUri)

	// Local backend's data lay in different cluster machines,
	// which should be handled separately
	if c.cfg.Backend.GetLocal() != nil {
		for _, addr := range c.hosts.GetAgents() {
			agent, err := clients.NewAgent(c.ctx, addr)
			if err != nil {
				return fmt.Errorf("create agent for %s failed: %w when clean local data",
					utils.StringifyAddr(addr), err)
			}

			// This is a hack, generally, we could not get local path
			// from uri by trimming directly
			backupPath := strings.TrimPrefix(backupUri, "local://")
			removeReq := &pb.RemoveDirRequest{
				Path: backupPath,
			}
			_, err = agent.RemoveDir(removeReq)
			if err != nil {
				return fmt.Errorf("remove %s in host: %s failed: %w", backupPath, addr.Host, err)
			}
			log.Debugf("Remove local data %s in %s successfully.", backupPath, addr.Host)
		}
	}

	return nil
}

func (c *Cleanup) Clean() error {
	logger := log.WithField("backup name", c.cfg.BackupName)

	logger.Info("Start to cleanup data in nebula cluster.")
	err := c.cleanNebula()
	if err != nil {
		log.Errorf("clean nebula local data failed: %v", err)
	}

	logger.Info("Start cleanup data in external storage.")
	err = c.cleanExternal()
	if err != nil {
		return fmt.Errorf("clean external storage data failed: %w", err)
	}

	logger.Info("Clean up backup data successfully.")
	return nil
}
