package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/cleanup"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
)

func NewCleanupCmd() *cobra.Command {
	cleanupCmd := &cobra.Command{
		Use:          "cleanup",
		Short:        "[EXPERIMENTAL]Clean up temporary files in backup",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := log.NewLogger(config.LogPath)

			defer logger.Sync() // flushes buffer, if any
			c := cleanup.NewCleanup(cleanupConfig, logger.Logger)

			err := c.Run()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cleanupCmd.PersistentFlags().StringVar(&cleanupConfig.BackupName, "backup_name", "", "backup name")
	cleanupCmd.MarkPersistentFlagRequired("backup_name")
	cleanupCmd.PersistentFlags().StringSliceVar(&cleanupConfig.MetaServer, "meta", nil, "meta server")
	cleanupCmd.MarkPersistentFlagRequired("meta")

	return cleanupCmd
}
