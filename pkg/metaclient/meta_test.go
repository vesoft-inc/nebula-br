package metaclient

import (
	"testing"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-go/nebula/meta"
	"go.uber.org/zap"
)

func TestOpen(t *testing.T) {
	logger, _ := zap.NewProduction()

	assert := assert.New(t)
	addr := "127.0.0.1:0"

	sock, err := thrift.NewServerSocket(addr)
	assert.Nil(err)

	var handler meta.MetaService
	processor := meta.NewMetaServiceProcessor(handler)
	server := thrift.NewSimpleServerContext(processor, sock)
	go server.Serve()
	time.Sleep(2 * time.Second)

	metaClient := NewMetaClient(logger)
	defer metaClient.Close()
	backupReq := meta.NewCreateBackupReq()

	_, err = metaClient.CreateBackup(backupReq)
	assert.NotNil(err)
	err = metaClient.Open(sock.Addr().String())
	assert.Nil(err)
	server.Stop()
}
