package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

func StringifyAddr(addr *nebula.HostAddr) string {
	if addr == nil {
		return "nil"
	}
	return fmt.Sprintf("%s:%d", addr.GetHost(), addr.GetPort())
}

func ParseAddr(addrStr string) (*nebula.HostAddr, error) {
	ipAddr := strings.Split(addrStr, ":")
	if len(ipAddr) != 2 {
		return nil, fmt.Errorf("bad format: %s", addrStr)
	}

	port, err := strconv.ParseInt(ipAddr[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("bad fomrat: %s", addrStr)
	}

	return &nebula.HostAddr{Host: ipAddr[0], Port: nebula.Port(port)}, nil
}

func StringifyBackup(b *meta.BackupMeta) string {
	m := map[string]string{
		"backup name":  string(b.GetBackupName()),
		"created time": time.Unix(b.GetCreateTime()/1000, 0).Local().String(),
		"all spaces":   strconv.FormatBool(b.GetAllSpaces()),
		"full backup":  strconv.FormatBool(b.GetFull()),
	}

	s := make([]string, 0, len(b.GetMetaFiles()))
	for _, f := range b.GetMetaFiles() {
		s = append(s, string(f))
	}
	m["meta files"] = strings.Join(s, ",")

	s = make([]string, 0, len(b.GetSpaceBackups()))
	for sid, backup := range b.GetSpaceBackups() {
		s = append(s, fmt.Sprintf("%s: space-id %d, hosts: %d", backup.GetSpace().GetSpaceName(), sid, len(backup.GetHostBackups())))
	}
	m["backups"] = strings.Join(s, ";")

	return fmt.Sprintf("%v", m)
}
