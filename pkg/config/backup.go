package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-br/pkg/storage"
)

func AddBackupFlags(flags *pflag.FlagSet) {
	flags.StringArray(FlagSpaces, nil,
		`(EXPERIMENTAL)space names.
    By this option, user can specify which spaces to backup. Now this feature is still experimental.
    If not specified, will backup all spaces.
    `)
	flags.String(FlagMetaAddr, "", "Specify meta server")
	cobra.MarkFlagRequired(flags, FlagMetaAddr)
	cobra.MarkFlagRequired(flags, FlagStorage)
}

type BackupConfig struct {
	MetaAddr string
	Spaces   []string
	Backend  *pb.Backend // Backend is associated with the root uri
}

func (b *BackupConfig) ParseFlags(flags *pflag.FlagSet) error {
	var err error
	b.MetaAddr, err = flags.GetString(FlagMetaAddr)
	if err != nil {
		return err
	}
	b.Spaces, err = flags.GetStringArray(FlagSpaces)
	if err != nil {
		return err
	}
	b.Backend, err = storage.ParseFromFlags(flags)
	if err != nil {
		return fmt.Errorf("parse storage flags failed: %w", err)
	}
	return nil
}
