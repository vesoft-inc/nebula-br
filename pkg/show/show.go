package show

import (
	"os"
	"os/exec"
	"strconv"
	"time"

	_ "github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/olekukonko/tablewriter"
	"github.com/vesoft-inc/nebula-br/pkg/storage"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
	_ "github.com/vesoft-inc/nebula-go/v2/nebula/meta"
	"go.uber.org/zap"
)

type Show struct {
	backend     storage.ExternalStorage
	backupFiles []string
	log         *zap.Logger
}

type showInfo struct {
	BackupName         string   `json:"name"`
	CreateTime         string   `json:"create_time"`
	Spaces             []string `json:"spaces"`
	Full               bool     `json:"full"`
	IncludeSystemSpace bool     `json:"specify_space"`
}

var tableHeader []string = []string{"name", "create_time", "spaces", "full_backup", "specify_space"}

func NewShow(backendUrl string, log *zap.Logger) *Show {
	backend, err := storage.NewExternalStorage(backendUrl, log, 5, "", nil)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	return &Show{log: log, backend: backend}
}

func (r *Show) readMetaFile(metaName string) ([]string, error) {
	filename := "/tmp/" + metaName
	m, err := utils.GetMetaFromFile(r.log, filename)
	if m == nil {
		r.log.Error("failed to get meta", zap.String("file", filename),
			zap.Error(err))
		return nil, err
	}

	var spaces string
	for _, s := range m.BackupInfo {
		if len(spaces) != 0 {
			spaces += ","
		}
		spaces += string(s.Space.SpaceName)
	}

	var info []string
	info = append(info, string(m.BackupName))
	info = append(info, time.Unix(0, m.CreateTime*int64(time.Millisecond)).Format("2006-01-02 15:04:05"))
	info = append(info, spaces)
	info = append(info, strconv.FormatBool(m.Full))

	info = append(info, strconv.FormatBool(m.IncludeSystemSpace))

	return info, nil
}

func (s *Show) showMetaFiles() ([][]string, error) {
	var asciiTable [][]string
	broken_info := []string{"", "backup is broken", "N/A", "N/A", "N/A"}

	for _, d := range s.backupFiles {
		metaFileName := d + ".meta"
		metaFile := d + "/" + metaFileName
		cmdStr := s.backend.RestoreMetaFileCommand(metaFile, "/tmp/")
		s.log.Info("download metafile", zap.Strings("cmd", cmdStr))
		cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
		err := cmd.Run()
		if err != nil {
			s.log.Error("cmd run failed", zap.Strings("run cmd", cmdStr), zap.Error(err))
			broken_info[0] = d
			broken_info[1] = broken_info[1] + ": backup meta file dowload error" // broken reason
			asciiTable = append(asciiTable, broken_info)
			continue
		}
		cmd.Wait()
		info, err := s.readMetaFile(metaFileName)
		if err != nil {
			s.log.Error("parse meta file failed", zap.Error(err))
			broken_info[0] = d
			broken_info[1] = broken_info[1] + ": meta file parse error, " + err.Error() // broken reason
			continue
		}
		asciiTable = append(asciiTable, info)
	}

	return asciiTable, nil
}

func (s *Show) ShowInfo() error {
	dirs, err := s.backend.ListBackupCommand()
	if err != nil {
		s.log.Error("list backup file failed", zap.Error(err))
		return err
	}

	s.backupFiles = dirs
	s.log.Info("list backup command return", zap.Strings("backup names", s.backupFiles))

	table, err := s.showMetaFiles()
	if err != nil {
		return err
	}

	tablewriter := tablewriter.NewWriter(os.Stdout)
	tablewriter.SetHeader(tableHeader)
	tablewriter.AppendBulk(table)
	tablewriter.Render()
	return nil
}
