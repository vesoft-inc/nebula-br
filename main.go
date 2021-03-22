package main

import (
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/cmd"
	"github.com/vesoft-inc/nebula-br/pkg/config"
)

func main() {

	rootCmd := &cobra.Command{
		Use:   "br",
		Short: "BR is a Nebula backup and restore tool",
	}
	rootCmd.AddCommand(cmd.NewBackupCmd(), cmd.NewVersionCmd(), cmd.NewRestoreCMD(), cmd.NewCleanupCmd(), cmd.NewShowCmd())
	rootCmd.PersistentFlags().StringVar(&config.LogPath, "log", "br.log", "log path")
	rootCmd.Execute()
}
