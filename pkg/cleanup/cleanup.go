package cleanup

import (
	"context"
	"fmt"

	"github.com/vesoft-inc/nebula-agent/pkg/storage"
	"github.com/vesoft-inc/nebula-br/pkg/clients"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/utils"

	log "github.com/sirupsen/logrus"
)

type Cleanup struct {
	ctx    context.Context
	cfg    config.CleanupConfig
	client *clients.NebulaMeta
	sto    storage.ExternalStorage
}

func NewCleanup(ctx context.Context, cfg config.CleanupConfig) (*Cleanup, error) {
	sto, err := storage.New(cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("create storage for %s failed: %w", cfg.Backend.Uri(), err)
	}

	client, err := clients.NewMeta(cfg.MetaAddr)
	if err != nil {
		return nil, fmt.Errorf("create meta client failed: %w", err)
	}

	return &Cleanup{
		ctx:    ctx,
		cfg:    cfg,
		client: client,
		sto:    sto,
	}, nil
}

func (c *Cleanup) cleanNebula() error {
	err := c.client.DropBackup([]byte(c.cfg.BackupName))
	if err != nil {
		return fmt.Errorf("drop backup failed: %w", err)
	}
	log.Debugf("Drop backup %s successfully", c.cfg.BackupName)

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
	return nil
}

func (c *Cleanup) Clean() error {
	logger := log.WithField("backup name", c.cfg.BackupName)

	logger.Info("Start to cleanup data in nebula cluster")
	err := c.cleanNebula()
	if err != nil {
		return fmt.Errorf("clean nebula local data failed: %w", err)
	}

	logger.Info("Start cleanup data in external storage")
	err = c.cleanExternal()
	if err != nil {
		return fmt.Errorf("clean external storage data failed: %w", err)
	}

	logger.Info("Clean up backup data successfully")
	return nil
}
