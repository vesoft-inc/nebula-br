package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/vesoft-inc/nebula-br/pkg/version"
)

func NewVersionCmd() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version of nebula br tool",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf(`%s,V-%d.%d.%d
   GitSha: %s
   GitRef: %s
please run "help" subcommand for more infomation.`,
				version.VerName, version.VerMajor, version.VerMinor, version.VerPatch,
				version.GitSha,
				version.GitRef)

			return nil
		},
	}
	return versionCmd
}
