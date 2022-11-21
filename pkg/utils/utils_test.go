package utils

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

func TestDumpParseBackup(t *testing.T) {
	assert := assert.New(t)

	files := [][]byte{
		[]byte("__edges__.sst"),
		[]byte("__index__.sst"),
		[]byte("__tags__.sst"),
	}
	backup := &meta.BackupMeta{
		SpaceBackups:   make(map[int32]*meta.SpaceBackupInfo),
		MetaFiles:      files,
		BackupName:     []byte("backup_test"),
		Full:           true,
		AllSpaces:      true,
		CreateTime:     time.Now().Unix(),
		BaseBackupName: []byte("base_backup_test"),
		StorageHosts:   make([]*nebula.HostAddr, 0),
	}

	err := EnsureDir(LocalTmpDir)
	assert.Nil(err, "Ensure local tmp dir failed", err)
	defer func() {
		err := RemoveDir(LocalTmpDir)
		assert.Nil(err, "Remove local tmp dir failed", err)
	}()

	tmpPath := filepath.Join(LocalTmpDir, "backup.meta")
	err = DumpMetaToFile(backup, tmpPath)
	assert.Nil(err, "Dump backup meta to file failed", err)

	backup1, err := ParseMetaFromFile(tmpPath)
	assert.Nil(err, "Parse backup meta from file failed", err)

	assert.True(reflect.DeepEqual(backup, backup1), "Backup meta are not consistent after dump and parse")
}

func TestUriJoin(t *testing.T) {
	assert := assert.New(t)

	root := "local://backup"
	uri, err := UriJoin(root, "BACKUP_NAME")
	assert.Nil(err, "Join uri failed", err)
	assert.Equal(uri, "local://backup/BACKUP_NAME", "Uri join does not works as expected")

	root = "s3://backup"
	uri, err = UriJoin(root, "root", "BACKUP_NAME")
	assert.Nil(err, "Join uri failed", err)
	assert.Equal(uri, "s3://backup/root/BACKUP_NAME", "Uri join does not works as expected")
}
