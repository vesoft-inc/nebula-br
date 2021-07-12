package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-br/pkg/backup"
	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/log"
)

func NewBackupCmd() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:          "backup",
		Short:        "backup Nebula Graph Database",
		SilenceUsage: true,
	}

	backupCmd.AddCommand(newFullBackupCmd())
	backupCmd.PersistentFlags().StringVar(&backupConfig.Meta, "meta", "", "meta server")
	backupCmd.PersistentFlags().StringArrayVar(&backupConfig.SpaceNames, "spaces", nil,
		`(EXPERIMENTAL)space names.
    By this option, user can specify which spaces to backup. Now this feature is still experimental.
    `)
	backupCmd.PersistentFlags().StringVar(&backupConfig.BackendUrl, "storage", "",
		`backup target url, format: <SCHEME>://<PATH>.
    <SCHEME>: a string indicating which backend type. optional: local, hdfs.
    now hdfs and local is supported, s3 and oss are still experimental.
    example:
    for local - "local:///the/local/path/to/backup"
    for hdfs  - "hdfs://example_host:example_port/examplepath"
    (EXPERIMENTAL) for oss - "oss://example/url/to/the/backup"
    (EXPERIMENTAL) for s3  - "s3://example/url/to/the/backup"
    `)
	backupCmd.PersistentFlags().StringVar(&backupConfig.User, "user", "", "username to login into the hosts where meta/storage service located")
	backupCmd.PersistentFlags().IntVar(&backupConfig.MaxSSHConnections, "connection", 5, "max ssh connection")
	backupCmd.PersistentFlags().IntVar(&backupConfig.MaxConcurrent, "concurrent", 5, "max concurrent(for aliyun OSS)")
	backupCmd.PersistentFlags().StringVar(&backupConfig.CommandArgs, "extra_args", "", "backup storage utils(oss/hdfs/s3) args for backup")
	backupCmd.PersistentFlags().BoolVar(&backupConfig.Verbose, "verbose", false, "show backup detailed informations")

	backupCmd.MarkPersistentFlagRequired("meta")
	backupCmd.MarkPersistentFlagRequired("storage")
	backupCmd.MarkPersistentFlagRequired("user")

	return backupCmd
}

func newFullBackupCmd() *cobra.Command {
	fullBackupCmd := &cobra.Command{
		Use:   "full",
		Short: "full backup Nebula Graph Database",
		Args: func(cmd *cobra.Command, args []string) error {

			if backupConfig.MaxSSHConnections <= 0 {
				backupConfig.MaxSSHConnections = 5
			}

			if backupConfig.MaxConcurrent <= 0 {
				backupConfig.MaxConcurrent = 5
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := log.NewLogger(config.LogPath)
			if err != nil {
				return err
			}
			defer logger.Sync() // flushes buffer, if any
			var b *backup.Backup
			b, err = backup.NewBackupClient(backupConfig, logger.Logger)
			if err != nil {
				return err
			}

			fmt.Println("start to backup cluster...")
			err = b.BackupCluster()
			if err != nil {
				return err
			}
			fmt.Println("backup successed.")
			if backupConfig.Verbose {
				b.ShowSummaries()
			}
			return nil
		},
	}

	return fullBackupCmd
}
