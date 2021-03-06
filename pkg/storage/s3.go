package storage

import (
	"bufio"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/vesoft-inc/nebula-br/pkg/context"
	"go.uber.org/zap"
)

type S3BackedStore struct {
	url        string
	log        *zap.Logger
	backupName string
	args       string
	command    string
}

func NewS3BackendStore(url string, log *zap.Logger, maxConcurrent int, args string, ctx *context.Context) *S3BackedStore {
	return &S3BackedStore{url: url, log: log, args: args}
}

func (s *S3BackedStore) SetBackupName(name string) {
	s.backupName = name
	if s.url[len(s.url)-1] != '/' {
		s.url += "/"
	}
	s.url += name
}

func (s *S3BackedStore) BackupPreCommand() []string {
	return nil
}

func (s *S3BackedStore) BackupStorageCommand(src []string, host string, spaceID string) []string {
	var cmd []string
	for i, dir := range src {
		storageDir := s.url + "/" + "storage/" + host + "/" + "data" + strconv.Itoa(i) + "/" + spaceID + "/"
		cmdStr := "aws " + s.args + " s3 sync " + dir + " " + storageDir
		cmd = append(cmd, cmdStr)
	}

	return cmd
}

func (s S3BackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.url + "/" + "meta/"
	return "aws " + s.args + " s3 sync " + filepath.Dir(src[0]) + " " + metaDir
}

func (s S3BackedStore) BackupMetaDir() string {
	return s.url + "/" + "meta"
}

func (s S3BackedStore) BackupMetaFileCommand(src string) []string {
	if len(s.args) == 0 {
		return []string{"aws", "s3", "cp", src, s.url + "/"}
	}
	args := strings.Fields(s.args)
	args = append(args, "s3", "cp", src, s.url+"/")
	args = append([]string{"aws"}, args...)
	return args
}

func (s S3BackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	if len(s.args) == 0 {
		return []string{"aws", "s3", "cp", s.url + "/" + file, dst}
	}
	args := strings.Fields(s.args)
	args = append(args, "s3", "cp", s.url+"/"+file, dst)
	args = append([]string{"aws"}, args...)
	return args
}

func (s S3BackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.url + "/" + "meta/"
	var sstFiles []string
	for _, f := range src {
		file := dst + "/" + f
		sstFiles = append(sstFiles, file)
	}
	return fmt.Sprintf("aws %s s3 sync %s "+dst, s.args, metaDir), sstFiles
}
func (s S3BackedStore) RestoreStorageCommand(host string, spaceID []string, dst []string) []string {
	var cmd []string
	for i, d := range dst {
		storageDir := s.url + "/storage/" + host + "/" + "data" + strconv.Itoa(i) + "/"
		cmdStr := fmt.Sprintf("aws %s s3 sync %s "+d, s.args, storageDir)
		cmd = append(cmd, cmdStr)
	}

	return cmd
}

func (s S3BackedStore) RestoreMetaPreCommand(srcDir string, bkDir string) string {
	return mvAndMkDirCommand(srcDir, bkDir)
}

func (s S3BackedStore) RestoreStoragePreCommand(srcDir string, bkDir string) string {
	return mvAndMkDirCommand(srcDir, bkDir)
}

func (s S3BackedStore) RestoreMetaPostCommand(bkDir string) string {
	return rmDirCommand(bkDir)
}

func (s S3BackedStore) RestoreStoragePostCommand(bkDir string) string {
	return rmDirCommand(bkDir)
}

func (s S3BackedStore) URI() string {
	return s.url
}
func (s S3BackedStore) Scheme() string {
	return SCHEME_S3
}

func (s S3BackedStore) CheckCommand() string {
	return "aws " + s.args + " s3 ls " + s.url
}

func (s S3BackedStore) ListBackupCommand() ([]string, error) {
	output, err := exec.Command("aws", "s3", "ls", s.url).Output()
	if err != nil {
		return nil, err
	}

	var dirs []string
	sc := bufio.NewScanner(strings.NewReader(string(output)))
	for sc.Scan() {
		w := strings.Fields(sc.Text())
		dirs = append(dirs, strings.TrimRight(w[1], "/"))
	}
	return dirs, nil
}
