package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
	"github.com/vesoft-inc/nebula-br/pkg/restore"
)

func NewRestoreCmd() *cobra.Command {
	restoreCmd := &cobra.Command{
		Use:          "restore",
		Short:        "restore Nebula Graph Database, notice that it will restart the cluster",
		SilenceUsage: true,
	}
	config.AddCommonFlags(restoreCmd.PersistentFlags())
	config.AddRestoreFlags(restoreCmd.PersistentFlags())
	restoreCmd.AddCommand(newFullRestoreCmd())
	return restoreCmd
}

func newFullRestoreCmd() *cobra.Command {
	fullRestoreCmd := &cobra.Command{
		Use:   "full",
		Short: "full restore Nebula Graph Database",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := log.SetLog(cmd.Flags())
			if err != nil {
				return fmt.Errorf("init logger failed: %w", err)
			}

			cfg := &config.RestoreConfig{}
			err = cfg.ParseFlags(cmd.Flags())
			if err != nil {
				return err
			}

			r, err := restore.NewRestore(context.TODO(), cfg)
			if err != nil {
				return err
			}

			err = r.Restore()
			if err != nil {
				return err
			}
			fmt.Println("restore succeed")
			return nil
		},
	}

	return fullRestoreCmd
}
