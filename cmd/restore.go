package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
	"github.com/vesoft-inc/nebula-br/pkg/restore"
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
	restoreCmd.PersistentFlags().IntVar(&restoreConfig.MaxConcurrent, "concurrent", 5, "max concurrent(for aliyun OSS)")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.CommandArgs, "extra_args", "", "storage utils(oss/hdfs/s3) args for restore")

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

			if restoreConfig.MaxConcurrent <= 0 {
				restoreConfig.MaxConcurrent = 5
			}

			return nil
		},

		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space
			logger, err := log.NewLogger(config.LogPath)
			if err != nil {
				return err
			}

			defer logger.Sync() // flushes buffer, if any

			var r *restore.Restore
			r, err = restore.NewRestore(restoreConfig, logger.Logger)
			if err != nil {
				return err
			}

			err = r.RestoreCluster()
			if err != nil {
				return err
			}
			fmt.Println("restore successed")
			return nil
		},
	}

	return fullRestoreCmd
}
