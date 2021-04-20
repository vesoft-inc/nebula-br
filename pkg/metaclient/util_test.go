package metaclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-go/nebula"
)

func TestHostaddrToString(t *testing.T) {
	assert := assert.New(t)
	host := nebula.NewHostAddr()
	host.Host = "192.168.8.1"
	host.Port = 80

	hostStr := HostaddrToString(host)
	assert.Equal(hostStr, "192.168.8.1:80")
}
