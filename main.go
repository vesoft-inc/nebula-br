package main

import (
	"github.com/spf13/cobra"

	"github.com/vesoft-inc/nebula-br/cmd"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "br",
		Short: "Nebula br is a Nebula backup and restore tool",
	}
	rootCmd.AddCommand(cmd.NewBackupCmd(), cmd.NewVersionCmd(), cmd.NewRestoreCmd(), cmd.NewCleanupCmd(), cmd.NewShowCmd())
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
