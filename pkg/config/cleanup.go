package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-br/pkg/storage"
)

func AddCleanupFlags(flags *pflag.FlagSet) {
	flags.String(FlagMetaAddr, "", "Specify meta server")
	flags.String(flagBackupName, "", "Specify backup name")
	cobra.MarkFlagRequired(flags, FlagMetaAddr)
	cobra.MarkFlagRequired(flags, flagBackupName)
	cobra.MarkFlagRequired(flags, FlagStorage)
}

type CleanupConfig struct {
	MetaAddr   string
	BackupName string
	Backend    *pb.Backend // Backend is associated with the root uri
}

func (c *CleanupConfig) ParseFlags(flags *pflag.FlagSet) error {
	var err error
	c.MetaAddr, err = flags.GetString(FlagMetaAddr)
	if err != nil {
		return err
	}
	c.BackupName, err = flags.GetString(flagBackupName)
	if err != nil {
		return err
	}
	c.Backend, err = storage.ParseFromFlags(flags)
	if err != nil {
		return fmt.Errorf("parse storage flags failed: %w", err)
	}
	return nil
}
