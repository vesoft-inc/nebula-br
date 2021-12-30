package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-br/pkg/storage"
)

const (
	flagConcurrency = "concurrency"
)

func AddRestoreFlags(flags *pflag.FlagSet) {
	flags.String(FlagMetaAddr, "", "Specify meta server")
	flags.String(flagBackupName, "", "Specify backup name")
	flags.Int(flagConcurrency, 5, "Max concurrency for download data") // TODO(spw): not use now

	cobra.MarkFlagRequired(flags, FlagMetaAddr)
	cobra.MarkFlagRequired(flags, FlagStorage)
	cobra.MarkFlagRequired(flags, flagBackupName)
}

type RestoreConfig struct {
	MetaAddr   string
	BackupName string
	Backend    *pb.Backend
}

func (r *RestoreConfig) ParseFlags(flags *pflag.FlagSet) error {
	var err error
	r.MetaAddr, err = flags.GetString(FlagMetaAddr)
	if err != nil {
		return err
	}
	r.BackupName, err = flags.GetString(flagBackupName)
	if err != nil {
		return err
	}
	r.Backend, err = storage.ParseFromFlags(flags)
	if err != nil {
		return fmt.Errorf("parse storage flags failed: %w", err)
	}
	return nil
}
