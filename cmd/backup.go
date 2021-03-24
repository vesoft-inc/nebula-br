package cmd

import (
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

	backupCmd.AddCommand(newFullBackupCmd())
	backupCmd.PersistentFlags().StringVar(&backupConfig.Meta, "meta", "", "meta server")
	backupCmd.PersistentFlags().StringArrayVar(&backupConfig.SpaceNames, "spaces", nil, "space names")
	backupCmd.PersistentFlags().StringVar(&backupConfig.BackendUrl, "storage", "", "storage path")
	backupCmd.PersistentFlags().StringVar(&backupConfig.User, "user", "", "user for meta/storage")
	backupCmd.PersistentFlags().IntVar(&backupConfig.MaxSSHConnections, "connection", 5, "max ssh connection")
	backupCmd.PersistentFlags().IntVar(&backupConfig.MaxConcurrent, "concurrent", 5, "max concurrent(for aliyun OSS)")

	backupCmd.MarkPersistentFlagRequired("meta")
	backupCmd.MarkPersistentFlagRequired("storage")
	backupCmd.MarkPersistentFlagRequired("user")

	return backupCmd
}

func newFullBackupCmd() *cobra.Command {
	fullBackupCmd := &cobra.Command{
		Use:   "full",
		Short: "full backup Nebula Graph Database",
		Args: func(cmd *cobra.Command, args []string) error {

			if backupConfig.MaxSSHConnections <= 0 {
				backupConfig.MaxSSHConnections = 5
			}

			if backupConfig.MaxConcurrent <= 0 {
				backupConfig.MaxConcurrent = 5
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := log.NewLogger(config.LogPath)
			if err != nil {
				return err
			}
			defer logger.Sync() // flushes buffer, if any
			b := backup.NewBackupClient(backupConfig, logger.Logger)

			err = b.BackupCluster()
			if err != nil {
				return err
			}
			fmt.Println("backup successed")
			return nil
		},
	}

	return fullBackupCmd
}
