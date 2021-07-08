package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-br/pkg/context"
	"go.uber.org/zap"
)

var logger, _ = zap.NewProduction()

func TestS3SetBackupName(t *testing.T) {
	s3 := NewS3BackendStore("s3://nebulabackup/", logger, 5, "--cli-read-timeout", &context.Context{})
	s3.SetBackupName("backupname1")
	assert.Equal(t, s3.backupName, "backupname1")

	assert.Equal(t, s3.URI(), "s3://nebulabackup/backupname1")

	s3 = NewS3BackendStore("s3://nebulabackup", logger, 5, "", &context.Context{})
	s3.SetBackupName("backupname2")
	assert.Equal(t, s3.backupName, "backupname2")

	assert.Equal(t, s3.URI(), "s3://nebulabackup/backupname2")
}

func TestS3StorageCommand(t *testing.T) {
	backupRegion := "s3://nebulabackup/"
	s3 := NewS3BackendStore(backupRegion, logger, 5, "--cli-read-timeout ", &context.Context{})
	s3.SetBackupName("backupname3")
	host := "127.0.0.1"
	spaceID := "1"
	dataDir := []string{"/home/nebula/"}
	cmd := s3.BackupStorageCommand(dataDir, host, spaceID)
	dst := s3.URI() + "/storage/" + host + "/" + "data0/" + spaceID + "/"
	assert.Equal(t, cmd[0], "aws --cli-read-timeout  s3 sync /home/nebula/ "+dst)

	cmd = s3.RestoreStorageCommand(host, []string{spaceID}, []string{"/home/data"})
	expectCmd := "aws --cli-read-timeout  s3 sync " + s3.URI() + "/storage/" + host + "/data0/" + " /home/data"
	assert.Equal(t, cmd[0], expectCmd)
}

func TestS3MetaCommand(t *testing.T) {
	s3 := NewS3BackendStore("s3://nebulabackup", logger, 5, "", &context.Context{})
	s3.SetBackupName("backupmeta")
	files := []string{"/data/a.sst", "/data/b.sst", "/data/c.sst"}
	cmd := s3.BackupMetaCommand(files)
	assert.Equal(t, cmd, "aws  s3 sync /data s3://nebulabackup/backupmeta/meta/")

	f := []string{"a.sst", "b.sst", "c.sst"}
	cmd, sstFiles := s3.RestoreMetaCommand(f, "/home/data")
	assert.Equal(t, cmd, "aws  s3 sync s3://nebulabackup/backupmeta/meta/ /home/data")
	assert.Equal(t, sstFiles, []string{"/home/data/a.sst", "/home/data/b.sst", "/home/data/c.sst"})
}

func TestS3BackupMetaFileCommand(t *testing.T) {
	backupRegion := "s3://nebulabackupfile/"
	s3 := NewS3BackendStore(backupRegion, logger, 5, "", &context.Context{})
	s3.SetBackupName("backupmetafile")
	metaFile := "/home/nebula/backup.meta"
	cmd := s3.BackupMetaFileCommand(metaFile)
	expectCmd := []string{"aws", "s3", "cp", metaFile, s3.URI() + "/"}
	assert.Equal(t, cmd, expectCmd)

	cmd = s3.RestoreMetaFileCommand("backup.meta", "/home/data")
	expectCmd = []string{"aws", "s3", "cp", "s3://nebulabackupfile/backupmetafile/backup.meta", "/home/data"}
	assert.Equal(t, cmd, expectCmd)
}
