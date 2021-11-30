package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
	"github.com/vesoft-inc/nebula-br/pkg/show"
)

var backendUrl string

func NewShowCmd() *cobra.Command {
	showCmd := &cobra.Command{
		Use:          "show",
		Short:        "show backup info",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := log.SetLog(cmd.Flags())
			if err != nil {
				return fmt.Errorf("init logger failed: %w", err)
			}

			cfg := &config.ShowConfig{}
			err = cfg.ParseFlags(cmd.Flags())
			if err != nil {
				return err
			}

			s, err := show.NewShow(context.TODO(), cfg)
			if err != nil {
				return err
			}

			err = s.Show()
			if err != nil {
				return err
			}

			return nil
		},
	}
	config.AddCommonFlags(showCmd.PersistentFlags())

	return showCmd
}
