package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/version"
)

func NewVersionCmd() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "print the version of nebula br tool",
		RunE: func(cmd *cobra.Command, args []string) error {
			vstring := fmt.Sprintf(
				`%s,V-%d.%d.%d
   GitSha: %s
   GitRef: %s
please run "help" subcommand for more infomation.`, version.VerName, version.VerMajor, version.VerMinor, version.VerPatch, version.GitSha, version.GitRef)

			fmt.Println(vstring)
			return nil
		},
	}
	return versionCmd
}
