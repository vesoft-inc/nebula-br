package utils

import (
	"flag"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-br/pkg/metaclient"
	"go.uber.org/zap"
)

var metafile = flag.String("meta", "", "metafile for test")

func TestIterateMetaBackup(t *testing.T) {
	ast := assert.New(t)
	metafname := *metafile
	logger, _ := zap.NewProduction()
	if metafname == "" {
		t.Log("meta should be provided!")
		return
	}

	m, err := GetMetaFromFile(logger, metafname)
	ast.Nil(err)

	IterateBackupMeta(m.GetBackupInfo(), ShowBackupMeta{})

	fmt.Printf("%s\n", metaclient.BackupMetaToString(m))
}
