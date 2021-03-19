package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/restore"
	"go.uber.org/zap"
)

func NewRestoreCMD() *cobra.Command {
	restoreCmd := &cobra.Command{
		Use:          "restore",
		Short:        "restore Nebula Graph Database",
		SilenceUsage: true,
	}

	restoreCmd.AddCommand(newFullRestoreCmd())
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.Meta, "meta", "", "meta server")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.BackendUrl, "storage", "", "storage path")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.User, "user", "", "user for meta and storage")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.BackupName, "name", "", "backup name")
	restoreCmd.PersistentFlags().IntVar(&restoreConfig.MaxConcurrent, "", 5, "max concurrent(for aliyun OSS)")

	restoreCmd.MarkPersistentFlagRequired("meta")
	restoreCmd.MarkPersistentFlagRequired("storage")
	restoreCmd.MarkPersistentFlagRequired("user")
	restoreCmd.MarkPersistentFlagRequired("name")

	return restoreCmd
}

func newFullRestoreCmd() *cobra.Command {
	fullRestoreCmd := &cobra.Command{
		Use:   "full",
		Short: "full restore Nebula Graph Database",
		Args: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewProduction()
			defer logger.Sync() // flushes buffer, if any

			if restoreConfig.MaxConcurrent <= 0 {
				restoreConfig.MaxConcurrent = 5
			}

			return nil
		},

		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any

			r := restore.NewRestore(restoreConfig, logger)
			err := r.RestoreCluster()
			if err != nil {
				return err
			}
			return nil
		},
	}

	return fullRestoreCmd
}
