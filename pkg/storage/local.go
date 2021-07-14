package storage

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/vesoft-inc/nebula-br/pkg/context"
	"go.uber.org/zap"
)

type LocalBackedStore struct {
	dir        string
	backupName string
	log        *zap.Logger
	args       string
	ctx        *context.Context
}

func NewLocalBackedStore(dir string, log *zap.Logger, maxConcurrent int, args string, ctx *context.Context) *LocalBackedStore {
	return &LocalBackedStore{dir: dir, log: log, args: args, ctx: ctx}
}

func (s *LocalBackedStore) SetBackupName(name string) {
	s.backupName = name
	s.dir += "/" + s.backupName
}

func (s LocalBackedStore) URI() string {
	return s.dir
}

func (s LocalBackedStore) Scheme() string {
	return SCHEME_LOCAL
}

func (s LocalBackedStore) copyCommand(src []string, dir string) string {
	cmdFormat := "mkdir -p " + dir + " && cp -rf %s %s " + dir
	files := ""
	for _, f := range src {
		files += f + " "
	}

	return fmt.Sprintf(cmdFormat, s.args, files)
}

func (s LocalBackedStore) remoteCopyCommand(src []string, dstHost string, dstDir string) string {
	cmdFormat := "scp -r %s %s " + dstHost + ":" + dstDir
	files := ""
	for _, f := range src {
		files += f + " "
	}

	return fmt.Sprintf(cmdFormat, s.args, files)
}

func (s *LocalBackedStore) BackupPreCommand() []string {
	return []string{"mkdir", s.dir}
}

func (s LocalBackedStore) backupMetaCommandLocalCopy(src []string) string {
	metaDir := s.BackupMetaDir()

	desturl := s.ctx.RemoteAddr
	desturl += ":"
	desturl += metaDir

	s.ctx.Reporter.MetaUploadingReport(s.ctx.RemoteAddr, src, desturl)

	return s.copyCommand(src, metaDir)
}

func (s LocalBackedStore) backupMetaCommandRemoteCopy(src []string) string {
	metaDir := s.BackupMetaDir()

	desturl := s.ctx.LocalAddr
	desturl += ":"
	desturl += metaDir

	s.ctx.Reporter.MetaUploadingReport(s.ctx.RemoteAddr, src, desturl)

	return s.remoteCopyCommand(src, s.ctx.LocalAddr, metaDir)
}

func (s LocalBackedStore) BackupMetaDir() string {
	return s.dir + "/" + "meta"
}

func (s LocalBackedStore) BackupMetaCommand(src []string) string {
	return s.backupMetaCommandRemoteCopy(src)
}

func (s LocalBackedStore) BackupStorageCommand(src []string, host string, spaceId string) []string {
	var cmd []string
	for i, dir := range src {
		storageDir := s.dir + "/" + "storage/" + host + "/" + "data" + strconv.Itoa(i) + "/" + spaceId //TODO(ywj): extract a common rule for tgt dir
		data := dir + "/data "
		wal := dir + "/wal "

		desturl := s.ctx.RemoteAddr
		desturl += ":"
		desturl += storageDir

		srcdirs := []string{data, wal}

		s.ctx.Reporter.StorageUploadingReport(spaceId, s.ctx.RemoteAddr, srcdirs, desturl)

		cmdStr := "mkdir -p " + storageDir + " && cp -rf " + s.args + " " + data + wal + storageDir
		cmd = append(cmd, cmdStr)
	}
	return cmd
}

func (s LocalBackedStore) BackupMetaFileCommand(src string) []string {
	if len(s.args) == 0 {
		return []string{"cp", src, s.dir}
	}
	args := strings.Fields(s.args)
	args = append(args, src, s.dir)
	args = append([]string{"cp"}, args...)
	return args
}

func (s LocalBackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	if len(s.args) == 0 {
		return []string{"cp", s.dir + "/" + file, dst}
	}
	args := strings.Fields(s.args)
	args = append(args, s.dir+"/"+file, dst)
	args = append([]string{"cp"}, args...)
	return args
}

func (s LocalBackedStore) restoreMetaCommandFromRemote(src []string, dst string) (string, []string) {
	metaDir := s.BackupMetaDir()
	files := ""
	var sstFiles []string
	for _, f := range src {
		file := metaDir + "/" + f
		srcFile := s.ctx.LocalAddr + ":" + file
		files += srcFile + " "

		dstFile := dst + "/" + f
		sstFiles = append(sstFiles, dstFile)
	}
	return fmt.Sprintf("scp -r %s %s %s", s.args, files, dst), sstFiles
}

func (s LocalBackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	return s.restoreMetaCommandFromRemote(src, dst)
}

func (s LocalBackedStore) RestoreStorageCommand(host string, spaceID []string, dst []string) []string {
	var cmd []string
	for i, d := range dst {
		storageDir := s.dir + "/storage/" + host + "/" + "data" + strconv.Itoa(i) + "/"
		dirs := ""
		for _, id := range spaceID {
			dirs += storageDir + id + " "
		}
		cmdStr := fmt.Sprintf("cp -rf %s %s %s", dirs, s.args, d)
		cmd = append(cmd, cmdStr)
	}

	return cmd
}

func (s LocalBackedStore) RestoreMetaPreCommand(dst string) string {
	//cleanup meta
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s LocalBackedStore) RestoreStoragePreCommand(dst string) string {
	//cleanup storage
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s LocalBackedStore) CheckCommand() string {
	return "ls " + s.dir
}

func (s LocalBackedStore) ListBackupCommand() ([]string, error) {
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}

	var backupFiles []string
	for _, f := range files {
		backupFiles = append(backupFiles, f.Name())
	}
	return backupFiles, nil
}
