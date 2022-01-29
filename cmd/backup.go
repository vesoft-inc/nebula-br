package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/vesoft-inc/nebula-br/pkg/backup"
	"github.com/vesoft-inc/nebula-br/pkg/cleanup"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
)

func NewBackupCmd() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:          "backup",
		Short:        "backup Nebula Graph Database to external storage for restore",
		SilenceUsage: true,
	}

	config.AddCommonFlags(backupCmd.PersistentFlags())
	config.AddBackupFlags(backupCmd.PersistentFlags())
	backupCmd.AddCommand(newFullBackupCmd())
	return backupCmd
}

func newFullBackupCmd() *cobra.Command {
	fullBackupCmd := &cobra.Command{
		Use:   "full",
		Short: "full backup Nebula Graph Database",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := log.SetLog(cmd.Flags())
			if err != nil {
				return fmt.Errorf("init logger failed: %w", err)
			}

			cfg := &config.BackupConfig{}
			err = cfg.ParseFlags(cmd.Flags())
			if err != nil {
				return fmt.Errorf("parse flags failed: %w", err)
			}

			b, err := backup.NewBackup(context.TODO(), cfg)
			if err != nil {
				return err
			}

			fmt.Println("start to backup cluster...")
			backupName, err := b.Backup()
			if err != nil {
				fmt.Println("backup failed, will try to clean the remaining garbage...")

				if backupName != "" {
					cleanCfg := &config.CleanupConfig{
						BackupName: backupName,
						Backend:    cfg.Backend,
						MetaAddr:   cfg.MetaAddr,
					}
					c, err := cleanup.NewCleanup(context.TODO(), cleanCfg)
					if err != nil {
						return fmt.Errorf("create cleanup for %s failed: %w", backupName, err)
					}

					err = c.Clean()
					if err != nil {
						return fmt.Errorf("cleanup %s failed when backup failed: %w", backupName, err)
					}
					fmt.Printf("cleanup backup %s successfully after backup failed", backupName)
				}
				return err
			}

			fmt.Println("backup succeed.")
			return nil
		},
	}

	return fullBackupCmd
}
