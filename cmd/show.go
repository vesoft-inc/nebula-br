package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
	"github.com/vesoft-inc/nebula-br/pkg/show"
	"go.uber.org/zap"
)

var backendUrl string

func NewShowCmd() *cobra.Command {
	showCmd := &cobra.Command{
		Use:          "show",
		Short:        "show backup info",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space

			logger, err := log.NewLogger(config.LogPath)
			if err != nil {
				return err
			}

			defer logger.Sync() // flushes buffer, if any

			s := show.NewShow(backendUrl, logger.Logger)

			err = s.ShowInfo()
			if err != nil {
				logger.Error("show info failed", zap.Error(err))
				return err
			}

			return nil
		},
	}

	showCmd.PersistentFlags().StringVar(&backendUrl, "storage", "", "storage path")

	showCmd.MarkPersistentFlagRequired("storage")

	return showCmd
}
