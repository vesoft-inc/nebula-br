package main

import (
	"github.com/spf13/cobra"
	"https://github.com/vesoft-inc/nebula-br/cmd"
)

func main() {

	rootCmd := &cobra.Command{
		Use:   "br",
		Short: "BR is a Nebula backup and restore tool",
	}
	rootCmd.AddCommand(cmd.NewBackupCmd(), cmd.NewVersionCmd(), cmd.NewRestoreCMD(), cmd.NewCleanupCmd())
	rootCmd.Execute()
}
