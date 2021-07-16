package metaclient

import (
	"encoding/json"
	"strconv"

	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

func HostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + strconv.Itoa(int(host.Port))
}

func BackupMetaToString(m *meta.BackupMeta) string {
	mstr, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(mstr)
}

func ListClusterInfoRespToString(m *meta.ListClusterInfoResp) string {
	mstr, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(mstr)
}
