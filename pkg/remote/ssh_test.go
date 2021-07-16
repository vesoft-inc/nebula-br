package remote

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	_ "golang.org/x/crypto/ssh"
)

var remoteAddr = flag.String("addr", "", "remote ssh addr for test")
var remoteUser = flag.String("user", "", "remote user for test")

func TestClient(t *testing.T) {
	ast := assert.New(t)
	if *remoteAddr == "" || *remoteUser == "" {
		t.Log("addr and user should be provided!")
		return
	}

	logger, _ := zap.NewProduction()
	cli, err := NewClient(*remoteAddr, *remoteUser, logger)
	ast.Nil(err)

	t.Logf("ssh user: %s", cli.client.Conn.User())
	t.Logf("local addr: %s, remote addr: %s",
		cli.client.Conn.LocalAddr().String(),
		cli.client.Conn.RemoteAddr().String())
}

func TestGetLocalAddress(t *testing.T) {
	ast := assert.New(t)
	if *remoteAddr == "" || *remoteUser == "" {
		t.Log("addr and user should be provided!")
		return
	}
	logger, _ := zap.NewProduction()
	laddr, err := GetAddresstoReachRemote(*remoteAddr, *remoteUser, logger)
	ast.Nil(err)

	t.Logf("local addr: %s", laddr)
}
