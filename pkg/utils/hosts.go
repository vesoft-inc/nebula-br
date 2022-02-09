package utils

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// NebulaHosts group all services(storaged/metad/graphd/listener) and agents by hostname or ip
type NebulaHosts struct {
	hosts map[string][]*meta.ServiceInfo // ip -> (agent, [storaged, metad, graphd, listener])
}

type HostDir struct {
	Host string // ip
	Dir  string // nebula root dir
}

func (h *NebulaHosts) String() string {
	if len(h.hosts) == 0 {
		return "nil"
	}

	m := make(map[string]string)
	for host, services := range h.hosts {
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

func (h *NebulaHosts) LoadFrom(resp *meta.ListClusterInfoResp) error {
	if resp.Code != nebula.ErrorCode_SUCCEEDED {
		return fmt.Errorf("response is not successful, code is %s", resp.GetCode().String())
	}

	h.hosts = resp.GetHostServices()

	// check only one agent in each host
	for _, services := range h.hosts {
		var agentAddr *nebula.HostAddr
		for _, s := range services {
			if s.GetRole() == meta.HostRole_AGENT {
				if agentAddr == nil {
					agentAddr = s.GetAddr()
				} else {
					return fmt.Errorf("there are more than one agent in host %s: %s, %s", s.GetAddr().GetHost(),
						StringifyAddr(agentAddr), StringifyAddr(s.GetAddr()))
				}
			}
		}
	}

	log.WithField("host info", h.String()).Info("Get cluster topology from the nebula.")
	return nil
}

func (h *NebulaHosts) StorageCount() int {
	if len(h.hosts) == 0 {
		return 0
	}

	c := 0
	for _, services := range h.hosts {
		for _, s := range services {
			if s.Role == meta.HostRole_STORAGE {
				c++
			}
		}
	}

	return c
}

// StoragePaths count storage services group by data path count
// path count -> services count having same paths count
func (h *NebulaHosts) StoragePaths() map[int]int {
	distribute := make(map[int]int)

	for _, services := range h.hosts {
		for _, s := range services {
			if s.Role == meta.HostRole_STORAGE {
				distribute[len(s.Dir.Data)]++
			}
		}
	}

	return distribute
}

func (h *NebulaHosts) HasService(addr *nebula.HostAddr) bool {
	if addr == nil {
		return false
	}

	services, ok := h.hosts[addr.GetHost()]
	if !ok {
		return false
	}

	for _, s := range services {
		if s.Addr.GetHost() != addr.GetHost() {
			log.WithField("should", addr.GetHost()).
				WithField("but", s.Addr.GetHost()).
				Infof("Wrong address %s in hosts map.", StringifyAddr(s.Addr))
			continue
		}

		if s.Addr.GetPort() == addr.GetPort() {
			return true
		}
	}

	return false
}

func (h *NebulaHosts) GetAgentFor(addr *nebula.HostAddr) (*nebula.HostAddr, error) {
	if !h.HasService(addr) {
		return nil, fmt.Errorf("service %s not found", StringifyAddr(addr))
	}

	services := h.hosts[addr.GetHost()]
	for _, s := range services {
		if s.Role == meta.HostRole_AGENT {
			return s.Addr, nil
		}
	}

	return nil, fmt.Errorf("do not find agent for service: %s", StringifyAddr(addr))
}

func (h *NebulaHosts) GetRootDirs() map[string][]*HostDir {
	hostRoots := make(map[string][]*HostDir)
	for host, services := range h.hosts {
		dirSet := make(map[string]bool)
		for _, s := range services {
			if s.Dir != nil && s.Dir.Root != nil {
				if len(s.Dir.Root) != 0 {
					dirSet[string(s.Dir.Root)] = true
				}
			}
		}

		var dirs []*HostDir
		for d := range dirSet {
			dirs = append(dirs, &HostDir{host, d})
		}
		hostRoots[host] = dirs
	}

	return hostRoots
}

func (h *NebulaHosts) GetHostServices() map[string][]*meta.ServiceInfo {
	return h.hosts
}

func (h *NebulaHosts) GetAgents() []*nebula.HostAddr {
	var al []*nebula.HostAddr
	for _, services := range h.hosts {
		for _, s := range services {
			if s.Role == meta.HostRole_AGENT {
				al = append(al, s.Addr)
			}
		}
	}

	return al
}

func (h *NebulaHosts) GetMetas() []*meta.ServiceInfo {
	var sl []*meta.ServiceInfo
	for _, services := range h.hosts {
		for _, s := range services {
			if s.Role == meta.HostRole_META {
				sl = append(sl, s)
			}
		}
	}

	return sl
}

func (h *NebulaHosts) GetStorages() []*meta.ServiceInfo {
	var sl []*meta.ServiceInfo
	for _, services := range h.hosts {
		for _, s := range services {
			if s.Role == meta.HostRole_STORAGE {
				sl = append(sl, s)
			}
		}
	}

	return sl
}

func (h *NebulaHosts) GetGraphs() []*meta.ServiceInfo {
	var gl []*meta.ServiceInfo
	for _, services := range h.hosts {
		for _, s := range services {
			if s.Role == meta.HostRole_GRAPH {
				gl = append(gl, s)
			}
		}
	}

	return gl
}
