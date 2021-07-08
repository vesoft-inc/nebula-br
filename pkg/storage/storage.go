package storage

import (
	"fmt"
	"net/url"

	"github.com/vesoft-inc/nebula-br/pkg/context"
	"go.uber.org/zap"
)

const (
	SCHEME_HDFS  = "hdfs"
	SCHEME_OSS   = "oss"
	SCHEME_S3    = "s3"
	SCHEME_LOCAL = "local"
)

type ExternalStorage interface {
	SetBackupName(name string)
	BackupPreCommand() []string
	BackupStorageCommand(src []string, host string, spaceID string) []string
	BackupMetaDir() string
	BackupMetaCommand(src []string) string
	BackupMetaFileCommand(src string) []string
	RestoreMetaFileCommand(file string, dst string) []string
	RestoreMetaCommand(src []string, dst string) (string, []string)
	RestoreStorageCommand(host string, spaceID []string, dst []string) []string
	RestoreMetaPreCommand(dst string) string
	RestoreStoragePreCommand(dst string) string
	CheckCommand() string
	ListBackupCommand() ([]string, error)
	URI() string
	Scheme() string
}

func NewExternalStorage(storageUrl string, log *zap.Logger, maxConcurrent int, args string, ctx *context.Context) (ExternalStorage, error) {
	u, err := url.Parse(storageUrl)
	if err != nil {
		return nil, err
	}

	log.Info("parsed external storage", zap.String("schema", u.Scheme), zap.String("path", u.Path))

	switch u.Scheme {
	case SCHEME_LOCAL:
		return NewLocalBackedStore(u.Path, log, maxConcurrent, args, ctx), nil
	case SCHEME_S3:
		return NewS3BackendStore(storageUrl, log, maxConcurrent, args, ctx), nil
	case SCHEME_OSS:
		return NewOSSBackendStore(storageUrl, log, maxConcurrent, args, ctx), nil
	case SCHEME_HDFS:
		return NewHDFSBackendStore(storageUrl, log, maxConcurrent, args, ctx), nil
	default:
		return nil, fmt.Errorf("Unsupported Backend Storage Types")
	}
}
