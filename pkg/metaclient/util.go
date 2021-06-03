package metaclient

import (
	"strconv"

	"github.com/vesoft-inc/nebula-go/v2/nebula"
)

func HostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + strconv.Itoa(int(host.Port))
}
