package show

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/vesoft-inc/nebula-agent/pkg/storage"
	_ "github.com/vesoft-inc/nebula-go/v2/nebula/meta"

	"github.com/vesoft-inc/nebula-br/pkg/config"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
)

type Show struct {
	ctx context.Context
	sto storage.ExternalStorage
	cfg *config.ShowConfig

	backupNames []string
}

type backupInfo struct {
	BackupName string   `json:"name"`
	CreateTime string   `json:"create_time"`
	Spaces     []string `json:"spaces"`
	Full       bool     `json:"full"`
	AllSpaces  bool     `json:"all_spaces"`
}

func (b *backupInfo) StringTable() []string {
	brokenInfo := []string{"", "backup is broken", "N/A", "N/A", "N/A"}
	if b == nil {
		return brokenInfo
	}

	table := brokenInfo
	table[0] = b.BackupName

	if b.CreateTime == "" {
		return table
	}
	table[1] = b.CreateTime

	if len(b.Spaces) == 0 {
		return table
	}
	table[2] = strings.Join(b.Spaces, ",")

	table[3] = strconv.FormatBool(b.Full)
	table[4] = strconv.FormatBool(b.AllSpaces)

	return table
}

var tableHeader = []string{"name", "create_time", "spaces", "full_backup", "all_spaces"}

func NewShow(ctx context.Context, cfg *config.ShowConfig) (*Show, error) {
	s, err := storage.New(cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("create external storage failed: %w", err)
	}

	dirNames, err := s.ListDir(ctx, cfg.Backend.Uri())
	if err != nil {
		return nil, fmt.Errorf("list dir failed: %w", err)
	}
	log.WithField("prefix", cfg.Backend.Uri()).WithField("backup names", dirNames).Debug("List backups")

	return &Show{
		ctx:         ctx,
		sto:         s,
		cfg:         cfg,
		backupNames: dirNames,
	}, nil
}

func (s *Show) downloadMetaFiles() (map[string]string, error) {
	metaFiles := make(map[string]string)
	for _, bname := range s.backupNames {
		bname = strings.Trim(bname, "/") // the s3 list result may have slashes

		if !utils.IsBackupName(bname) {
			log.Infof("%s is not backup name", bname)
			continue
		}

		metaName := bname + ".meta"
		localTmpPath := filepath.Join(utils.LocalTmpDir, metaName)
		externalUri, _ := utils.UriJoin(s.cfg.Backend.Uri(), bname, metaName)

		err := s.sto.Download(s.ctx, localTmpPath, externalUri, false)
		if err != nil {
			log.WithError(err).Infof("download %s to %s failed", externalUri, localTmpPath)
		} else {
			log.WithField("external", externalUri).WithField("local", localTmpPath).Debug("Download backup meta file successfully.")
		}

		metaFiles[bname] = localTmpPath
	}

	return metaFiles, nil
}

func (s *Show) parseMetaFiles(metaPaths map[string]string) ([]*backupInfo, error) {
	var infoList []*backupInfo
	for name, path := range metaPaths {
		log.WithField("meta path", path).Debug("Start parse meta file")
		m, err := utils.ParseMetaFromFile(path)
		if err != nil || m == nil {
			log.WithError(err).WithField("meta path", path).Error("parse meta file failed")
			infoList = append(infoList, &backupInfo{BackupName: name})
			continue
		}

		if name != string(m.BackupName) {
			log.Errorf("Name from path: %s and name parsed from meta: %s are not consistent", name, string(m.BackupName))
		}

		spaces := make([]string, 0)
		for _, b := range m.GetSpaceBackups() {
			spaces = append(spaces, string(b.Space.SpaceName))
		}

		info := &backupInfo{
			BackupName: string(m.BackupName),
			CreateTime: time.Unix(0, m.CreateTime*int64(time.Millisecond)).Format("2006-01-02 15:04:05"),
			Spaces:     spaces,
			Full:       m.Full,
			AllSpaces:  m.AllSpaces,
		}

		infoList = append(infoList, info)
	}

	return infoList, nil
}

func (s *Show) showBackupInfo(infoList []*backupInfo) {
	asciiTable := make([][]string, 0)
	for _, info := range infoList {
		asciiTable = append(asciiTable, info.StringTable())
	}

	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader(tableHeader)
	tw.AppendBulk(asciiTable)
	tw.Render()
}

func (s *Show) Show() error {
	logger := log.WithField("root", s.cfg.Backend.Uri())

	if err := utils.EnsureDir(utils.LocalTmpDir); err != nil {
		return err
	}
	defer func() {
		if err := utils.RemoveDir(utils.LocalTmpDir); err != nil {
			log.WithError(err).Errorf("Remove tmp dir %s failed", utils.LocalTmpDir)
		}
	}()

	logger.Debug("Start download backup meta files.")
	files, err := s.downloadMetaFiles()
	if err != nil {
		return err
	}

	logger.Debug("Start parse backup meta files.")
	infoList, err := s.parseMetaFiles(files)
	if err != nil {
		return err
	}

	logger.Debug("Start show meta info.")
	s.showBackupInfo(infoList)
	return nil
}
