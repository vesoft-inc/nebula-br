package clients

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

type NebulaMeta struct {
	client     *meta.MetaServiceClient
	leaderAddr *nebula.HostAddr
}

func NewMeta(addrStr string) (*NebulaMeta, error) {
	addr, err := utils.ParseAddr(addrStr)
	if err != nil {
		return nil, err
	}

	m := &NebulaMeta{
		leaderAddr: addr,
	}

	if m.client, err = connect(addr); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *NebulaMeta) LeaderAddr() *nebula.HostAddr {
	return m.leaderAddr
}

func (m *NebulaMeta) reconnect(addr *nebula.HostAddr) error {
	if addr == meta.ExecResp_Leader_DEFAULT {
		return fmt.Errorf("leader not found when call ListCluster")
	}
	m.client.Close()

	var err error
	c, err := connect(addr)
	if err != nil {
		return fmt.Errorf("connect to new meta client leader %s failed: %w",
			utils.StringifyAddr(addr), err)
	}

	m.leaderAddr = addr
	m.client = c
	return nil
}

func (m *NebulaMeta) ListCluster() (*meta.ListClusterInfoResp, error) {
	req := &meta.ListClusterInfoReq{}

	for {
		resp, err := m.client.ListCluster(req)
		if err != nil {
			return nil, fmt.Errorf("list cluster %s failed: %w", utils.StringifyAddr(m.leaderAddr), err)
		}

		// retry only when leader change
		if resp.GetCode() == nebula.ErrorCode_E_LEADER_CHANGED {
			err := m.reconnect(resp.GetLeader())
			if err != nil {
				return nil, err
			}
			continue
		}

		// fill the meta dir info
		for _, services := range resp.GetHostServices() {
			for _, s := range services {
				if s.Role == meta.HostRole_META {
					dir, err := m.getMetaDirInfo(s.GetAddr())
					if err != nil {
						return nil, fmt.Errorf("get meta %s from address failed: %w",
							utils.StringifyAddr(s.GetAddr()), err)
					}
					s.Dir = dir
				}
			}
		}

		return resp, nil
	}
}

func (m *NebulaMeta) CreateBackup(spaces []string) (*meta.CreateBackupResp, error) {
	req := meta.NewCreateBackupReq()
	if spaces != nil || len(spaces) != 0 {
		req.Spaces = make([][]byte, 0, len(spaces))
		for _, space := range spaces {
			req.Spaces = append(req.Spaces, []byte(space))
		}
	}

	for {
		resp, err := m.client.CreateBackup(req)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() == nebula.ErrorCode_E_LEADER_CHANGED {
			err := m.reconnect(resp.GetLeader())
			if err != nil {
				return nil, err
			}
			continue
		}

		return resp, nil
	}

}

func (m *NebulaMeta) DropBackup(name []byte) error {
	req := meta.NewDropSnapshotReq()
	req.Name = name

	for {
		resp, err := m.client.DropSnapshot(req)
		if err != nil {
			return fmt.Errorf("call drop snapshot failed: %w", err)
		}

		if resp.GetCode() == nebula.ErrorCode_E_LEADER_CHANGED {
			err := m.reconnect(resp.GetLeader())
			if err != nil {
				return err
			}
			continue
		}

		if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
			return nil
		}
		return fmt.Errorf("call drop snapshot failed: %s", resp.GetCode().String())
	}

}

func (m *NebulaMeta) GetSpace(space []byte) (*meta.GetSpaceResp, error) {
	req := meta.NewGetSpaceReq()
	req.SpaceName = space

	for {
		resp, err := m.client.GetSpace(req)
		if err != nil {
			return nil, err
		}

		if resp.GetCode() == nebula.ErrorCode_E_LEADER_CHANGED {
			err := m.reconnect(resp.GetLeader())
			if err != nil {
				return nil, err
			}
			continue
		}

		return resp, nil
	}
}

func (m *NebulaMeta) DropSpace(space []byte, ifExists bool) error {
	req := meta.NewDropSpaceReq()
	req.SpaceName = space
	req.IfExists = ifExists

	for {
		resp, err := m.client.DropSpace(req)
		if err != nil {
			return err
		}

		if resp.GetCode() == nebula.ErrorCode_E_LEADER_CHANGED {
			err := m.reconnect(resp.GetLeader())
			if err != nil {
				return err
			}
			continue
		}

		if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
			return nil
		}
		return fmt.Errorf("call DropSpace failed: %s", resp.GetCode().String())
	}
}

// single metad node
func (m *NebulaMeta) RestoreMeta(metaAddr *nebula.HostAddr, hostMap []*meta.HostPair, files []string) error {
	byteFiles := make([][]byte, 0, len(files))
	for _, f := range files {
		byteFiles = append(byteFiles, []byte(f))
	}
	req := meta.NewRestoreMetaReq()
	req.Hosts = hostMap
	req.Files = byteFiles

	for try := 1; try <= 3; try++ {
		client, err := connect(metaAddr)
		if err != nil {
			log.WithError(err).WithField("addr", utils.StringifyAddr(metaAddr)).
				Errorf("connect to metad failed, try times %d", try)
			time.Sleep(time.Second * 2)
			continue
		}

		resp, err := client.RestoreMeta(req)
		if err != nil {
			log.WithError(err).WithField("req", req).Error("Restore meta failed")
			return err
		}

		if resp.GetCode() == nebula.ErrorCode_SUCCEEDED {
			return nil
		}
		return fmt.Errorf("call %s:RestoreMeta failed: %s",
			utils.StringifyAddr(metaAddr), resp.GetCode().String())
	}

	return fmt.Errorf("try to connect %s 3 times, but failed", utils.StringifyAddr(metaAddr))
}

func (m *NebulaMeta) getMetaDirInfo(addr *nebula.HostAddr) (*nebula.DirInfo, error) {
	log.WithField("addr", utils.StringifyAddr(addr)).Debug("Try to get dir info from meta service")
	c, err := connect(addr)
	if err != nil {
		return nil, err
	}

	defer func() {
		e := c.Close()
		if e != nil {
			log.WithError(e).WithField("host", addr.String()).Error("Close error when get meta dir info.")
		}
	}()

	req := &meta.GetMetaDirInfoReq{}
	resp, err := c.GetMetaDirInfo(req)
	if err != nil {
		return nil, err
	}

	if resp.GetCode() != nebula.ErrorCode_SUCCEEDED {
		return nil, fmt.Errorf("get meta dir info failed: %v", resp.GetCode())
	}

	return resp.GetDir(), nil
}
