package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/backup"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
)

func NewBackupCmd() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:          "backup",
		Short:        "backup Nebula Graph Database",
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
			err = b.Backup()
			if err != nil {
				return err
			}
			fmt.Println("backup successed.")
			return nil
		},
	}

	return fullBackupCmd
}
