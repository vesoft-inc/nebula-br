package storage

import (
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-br/pkg/context"
	"github.com/vesoft-inc/nebula-br/pkg/remote"
	"go.uber.org/zap"
)

func TestStorage(t *testing.T) {
	assert := assert.New(t)
	logger, _ := zap.NewProduction()
	s, err := NewExternalStorage("local:///tmp/backup", logger, 5, "", &context.Context{})
	assert.NoError(err)
	assert.Equal(reflect.TypeOf(s).String(), "*storage.LocalBackedStore")

	assert.Equal(s.URI(), "/tmp/backup")

	s, err = NewExternalStorage("s3://nebulabackup/", logger, 5, "", &context.Context{})
	assert.NoError(err)

	assert.Equal(s.URI(), "s3://nebulabackup/")

	s, err = NewExternalStorage("oss://nebulabackup/", logger, 5, "", &context.Context{})
	assert.NoError(err)
}

var userName = flag.String("user", "", "user for test")

func TestStorageCmdMv(t *testing.T) {
	ast := assert.New(t)
	logger, _ := zap.NewProduction()
	if *userName == "" {
		t.Log("should provide username")
		return
	}
	dir := "/tmp/testdir"

	cmd := exec.Command("mkdir", "-p", dir)
	err := cmd.Run()
	var cli *remote.Client

	ast.Nil(err)

	tname := "test.txt"
	tcontent := "some.sample.content"

	file, _ := os.OpenFile(dir+"/"+tname, os.O_RDWR|os.O_CREATE, 0755)
	file.WriteString(tcontent)
	file.Close()

	bakpath := getBackDir(dir)
	t.Logf("dir: %s , bakpath: %s", dir, bakpath)

	cli, err = remote.NewClient("127.0.0.1", *userName, logger)
	ast.Nil(err)

	mvCmd := mvDirCommand(dir, bakpath)
	err = cli.ExecCommandBySSH(mvCmd)
	ast.Nil(err)

	dat, _ := ioutil.ReadFile(bakpath + "/" + tname)
	ast.Equal(string(dat), tcontent)
}

func TestStorageCmdEmpty(t *testing.T) {
	ast := assert.New(t)
	m1 := mvDirCommand("", "t")
	m2 := mvDirCommand("t", "")
	m3 := mvDirCommand("", "")
	ast.Equal(m1, "")
	ast.Equal(m2, "")
	ast.Equal(m3, "")

	r1 := rmDirCommand("")
	ast.Equal(r1, "")

	k1 := mkDirCommand("")
	ast.Equal(k1, "")
}

func TestRmCmdCheck(t *testing.T) {
	ast := assert.New(t)
	invalidDsts := []string{
		"/", "/abc/test /", "/data", "/some/sample/path_old_12345 ////",
	}
	for _, i := range invalidDsts {
		res := sanityCheckForRM(i)
		t.Logf("check '%s' ==> %v", i, res)
		ast.False(res)
	}

	validDst := getBackDir("/some/data/path")
	res := sanityCheckForRM(validDst)
	t.Logf("check '%s' ==> %v", validDst, res)
	ast.True(res)
}
