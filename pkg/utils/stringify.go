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

	return &nebula.HostAddr{ipAddr[0], nebula.Port(port)}, nil
}

func StringifyBackup(b *meta.BackupMeta) string {
	m := make(map[string]string)
	m["backup name"] = string(b.GetBackupName())
	m["created time"] = time.Unix(b.GetCreateTime()/1000, 0).Local().String()
	m["all spaces"] = fmt.Sprintf("%v", b.GetAllSpaces())
	m["full backup"] = fmt.Sprintf("%v", b.GetFull())

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

func StringifyClusterInfo(info *meta.ListClusterInfoResp) string {
	m := make(map[string]string)

	for host, services := range info.GetHostServices() {
		ss := make([]string, 0)
		for _, s := range services {
			dataPaths := make([]string, 0)
			for _, d := range s.GetDir().GetData() {
				dataPaths = append(dataPaths, string(d))
			}
			ss = append(ss, fmt.Sprintf("%s[%s]: (data: %s, root: %s)",
				StringifyAddr(s.GetAddr()), s.GetRole(), strings.Join(dataPaths, ","), string(s.GetDir().GetRoot())))
		}
		m[host] = strings.Join(ss, " | ")
	}

	return fmt.Sprintf("%v", m)
}
