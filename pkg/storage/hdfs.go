package storage

import (
	"bufio"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/vesoft-inc/nebula-br/pkg/context"
	"go.uber.org/zap"
)

type HDFSBackedStore struct {
	url        string
	log        *zap.Logger
	backupName string
	args       string
	command    string
}

func NewHDFSBackendStore(url string, log *zap.Logger, maxConcurrent int, args string, ctx *context.Context) *HDFSBackedStore {
	return &HDFSBackedStore{url: url, log: log, args: args}
}

func (s *HDFSBackedStore) SetBackupName(name string) {
	if s.url[len(s.url)-1] != '/' {
		s.url += "/"
	}
	s.url += name
	s.backupName = name
}

func (s HDFSBackedStore) URI() string {
	return s.url
}
func (s HDFSBackedStore) Scheme() string {
	return SCHEME_HDFS
}

func (s HDFSBackedStore) copyCommand(src []string, dir string) string {
	cmdFormat := "hadoop fs -mkdir -p " + dir + " && hadoop fs -copyFromLocal  %s %s " + dir
	files := ""
	for _, f := range src {
		files += f + " "
	}

	return fmt.Sprintf(cmdFormat, s.args, files)
}

func (s *HDFSBackedStore) BackupPreCommand() []string {
	return []string{"hadoop", "fs", "-mkdir", s.url}
}

func (s HDFSBackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.url + "/" + "meta"
	return s.copyCommand(src, metaDir)
}

func (s HDFSBackedStore) BackupMetaDir() string {
	return s.url + "/" + "meta"
}

func (s HDFSBackedStore) BackupStorageCommand(src []string, host string, spaceId string) []string {
	var cmd []string
	hosts := strings.Split(host, ":")
	for i, dir := range src {
		storageDir := s.url + "/" + "storage/" + hosts[0] + "/" + hosts[1] + "/" + "data" + strconv.Itoa(i) + "/" + spaceId
		data := dir + "/data "
		wal := dir + "/wal "
		cmdStr := "hadoop fs -mkdir -p " + storageDir + " && hadoop fs -copyFromLocal " + s.args + " " + data + wal + storageDir
		cmd = append(cmd, cmdStr)
	}
	return cmd
}

func (s HDFSBackedStore) BackupMetaFileCommand(src string) []string {
	if len(s.args) == 0 {
		return []string{"hadoop", "fs", "-copyFromLocal", src, s.url}
	}
	args := strings.Fields(s.args)
	args = append(args, src, s.url)
	args = append([]string{"hadoop", "fs", "-copyFromLocal"}, args...)
	return args
}

func (s HDFSBackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	if len(s.args) == 0 {
		return []string{"hadoop", "fs", "-copyToLocal", "-f", s.url + "/" + file, dst}
	}
	args := strings.Fields(s.args)
	args = append(args, s.url+"/"+file, dst)
	args = append([]string{"hadoop", "fs", "-copyToLocal", "-f"}, args...)
	return args
}

func (s HDFSBackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.url + "/" + "meta/"
	files := ""
	var sstFiles []string
	for _, f := range src {
		file := metaDir + f
		files += file + " "
		dstFile := dst + "/" + f
		sstFiles = append(sstFiles, dstFile)
	}
	return fmt.Sprintf("hadoop fs -copyToLocal -f %s %s %s", files, s.args, dst), sstFiles
}

func (s HDFSBackedStore) RestoreStorageCommand(host string, spaceID []string, dst []string) []string {
	hosts := strings.Split(host, ":")
	var cmd []string
	for i, d := range dst {
		storageDir := s.url + "/storage/" + hosts[0] + "/" + hosts[1] + "/" + "data" + strconv.Itoa(i) + "/"
		dirs := ""
		for _, id := range spaceID {
			dirs += storageDir + id + " "
		}
		cmdStr := fmt.Sprintf("hadoop fs -copyToLocal %s %s %s", dirs, s.args, d)
		cmd = append(cmd, cmdStr)
	}

	return cmd
}

func (s HDFSBackedStore) RestoreMetaPreCommand(dst string) string {
	//cleanup meta
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s HDFSBackedStore) RestoreStoragePreCommand(dst string) string {
	//cleanup storage
	return "rm -rf " + dst + " && mkdir -p " + dst
}

func (s HDFSBackedStore) CheckCommand() string {
	return "hadoop fs -ls " + s.url
}

func (s HDFSBackedStore) ListBackupCommand() ([]string, error) {
	output, err := exec.Command("hadoop", "fs", "-ls", "-C", s.url).Output()
	if err != nil {
		return nil, err
	}

	var dirs []string
	sc := bufio.NewScanner(strings.NewReader(string(output)))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "hdfs://") {
			break
		}
		index := strings.Index(line, s.url)
		if index == -1 {
			return nil, fmt.Errorf("Wrong hdfs file name %s", line)
		}
		dirs = append(dirs, line[len(s.url):])
	}

	return dirs, nil
}
