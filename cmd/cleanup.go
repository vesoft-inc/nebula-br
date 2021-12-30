package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/cleanup"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
)

func NewCleanupCmd() *cobra.Command {
	cleanupCmd := &cobra.Command{
		Use:          "cleanup",
		Short:        "Cleanup backup files in external storage and nebula cluster",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := log.SetLog(cmd.Flags())
			if err != nil {
				return fmt.Errorf("init logger failed: %w", err)
			}

			cfg := config.CleanupConfig{}
			err = cfg.ParseFlags(cmd.Flags())
			if err != nil {
				return fmt.Errorf("parse flags failed")
			}

			c, err := cleanup.NewCleanup(context.TODO(), cfg)
			if err != nil {
				return err
			}

			err = c.Clean()
			if err != nil {
				return err
			}

			return nil
		},
	}

	config.AddCommonFlags(cleanupCmd.PersistentFlags())
	config.AddCleanupFlags(cleanupCmd.PersistentFlags())
	return cleanupCmd
}
