package cmd

import "github.com/vesoft-inc/nebula-br/pkg/config"

var (
	backupConfig  config.BackupConfig
	restoreConfig config.RestoreConfig
	// for cleanup
	cleanupConfig config.CleanupConfig
)
