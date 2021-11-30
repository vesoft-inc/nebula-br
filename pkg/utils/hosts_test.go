package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

var (
	nebulaRoot = []byte("/home/nebula/nebula-install")
	nebulaMeta = [][]byte{
		[]byte("/home/nebula/nebula-install/data/meta"),
	}
	nebulaStorage = [][]byte{
		[]byte("/home/nebula/nebula-install/data/storage"),
	}
)

func parseAddrNoErr(t *testing.T, addrStr string) *nebula.HostAddr {
	assert := assert.New(t)
	addr, err := ParseAddr(addrStr)
	assert.Nil(err, "parse address from string failed", err)
	return addr
}

func TestHosts(t *testing.T) {
	assert := assert.New(t)

	localHost := "127.0.0.1"
	metaAddr := parseAddrNoErr(t, "127.0.0.1:9559")
	graphAddr := parseAddrNoErr(t, "127.0.0.1:9669")
	storageAddr := parseAddrNoErr(t, "127.0.0.1:9779")
	agentAddr := parseAddrNoErr(t, "127.0.0.1:8888")
	host2 := "127.0.0.2"
	metaAddr2 := parseAddrNoErr(t, "127.0.0.2:9559")

	metad := &meta.ServiceInfo{
		Dir:  nebula.NewDirInfo().SetData(nebulaMeta).SetRoot(nebulaRoot),
		Role: meta.HostRole_META,
		Addr: metaAddr,
	}
	graphd := &meta.ServiceInfo{
		Dir:  nebula.NewDirInfo().SetRoot(nebulaRoot),
		Role: meta.HostRole_GRAPH,
		Addr: graphAddr,
	}
	storaged := &meta.ServiceInfo{
		Dir:  nebula.NewDirInfo().SetData(nebulaStorage).SetRoot(nebulaRoot),
		Role: meta.HostRole_STORAGE,
		Addr: storageAddr,
	}
	agent := &meta.ServiceInfo{
		Dir:  nebula.NewDirInfo().SetRoot(nebulaRoot),
		Role: meta.HostRole_AGENT,
		Addr: agentAddr,
	}
	metad2 := &meta.ServiceInfo{
		Dir:  nebula.NewDirInfo().SetData(nebulaMeta).SetRoot(nebulaRoot),
		Role: meta.HostRole_META,
		Addr: metaAddr2,
	}

	resp := &meta.ListClusterInfoResp{
		HostServices: map[string][]*meta.ServiceInfo{
			localHost: {
				metad,
				graphd,
				storaged,
				agent,
			},
			host2: {
				metad2,
			},
		},
		Code: nebula.ErrorCode_SUCCEEDED,
	}

	h := &NebulaHosts{}
	err := h.LoadFrom(resp)
	assert.Nil(err, "Load from list cluster info response failed", err)

	// topology check logic
	assert.Equal(h.StorageCount(), 1)
	assert.Equal(h.StoragePaths(), map[int]int{1: 1})

	// check service
	assert.True(h.HasService(metaAddr))
	assert.True(h.HasService(metaAddr2))
	assert.True(h.HasService(graphAddr))
	assert.True(h.HasService(storageAddr))
	assert.True(h.HasService(agentAddr))

	// check service's agent
	addr1, err := h.GetAgentFor(metaAddr)
	assert.Nil(err)
	assert.Equal(addr1, agentAddr)
	addr2, err := h.GetAgentFor(graphAddr)
	assert.Nil(err)
	assert.Equal(addr2, agentAddr)
	addr3, err := h.GetAgentFor(agentAddr)
	assert.Nil(err)
	assert.Equal(addr3, agentAddr)

	addr4, err := h.GetAgentFor(metaAddr2)
	assert.NotNil(err)
	assert.Nil(addr4)

	// check dirs
	assert.Equal(h.GetRootDirs(), map[string][]*HostDir{
		localHost: {&HostDir{localHost, string(nebulaRoot)}},
		host2:     {&HostDir{host2, string(nebulaRoot)}},
	})

	// check service list
	assert.Equal(h.GetAgents(), []*nebula.HostAddr{agentAddr})
	assert.Equal(h.GetMetas(), []*meta.ServiceInfo{metad, metad2})
	assert.Equal(h.GetGraphs(), []*meta.ServiceInfo{graphd})
	assert.Equal(h.GetStorages(), []*meta.ServiceInfo{storaged})
}
