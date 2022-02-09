package restore

import (
	"fmt"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"

	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-br/pkg/clients"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

type Fix struct {
	r        *Restore
	hosts    *utils.NebulaHosts
	agentMgr *clients.AgentManager

	backSuffix string
}

func NewFixFrom(r *Restore) (*Fix, error) {
	if r.hosts == nil || r.agentMgr == nil {
		return nil, fmt.Errorf("empty hosts or agents manager")
	}

	return &Fix{
		r:          r,
		hosts:      r.hosts,
		agentMgr:   r.agentMgr,
		backSuffix: r.backSuffix,
	}, nil
}

// Move back the data dir in restore process
func (f *Fix) fixData() error {
	services := f.hosts.GetStorages()
	services = append(services, f.hosts.GetMetas()...)

	for _, s := range services {
		name := fmt.Sprintf("%s[%s]", s.GetRole().String(), utils.StringifyAddr(s.GetAddr()))
		logger := log.WithField("name", name)

		agent, err := f.agentMgr.GetAgentFor(s.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for %s failed: %w", name, err)
		}

		if s.GetRole() == meta.HostRole_META && len(s.Dir.Data) != 1 {
			return fmt.Errorf("meta service: %s should only have one data dir, but %d",
				name, len(s.Dir.Data))
		}

		for _, d := range s.Dir.Data {
			opath := filepath.Join(string(d), "nebula")
			bpath := fmt.Sprintf("%s%s", opath, f.backSuffix)

			// check if the old data exist
			existReq := &pb.ExistDirRequest{
				Path: bpath,
			}
			res, err := agent.ExistDir(existReq)
			if err != nil {
				return fmt.Errorf("check %s:%s exist failed: %w", name, opath, err)
			}
			if !res.Exist {
				logger.WithField("path", bpath).Debug("Origin backup storage data path does not exist, skip it")
				continue
			}

			// remove the newly downloaded data
			rmReq := &pb.RemoveDirRequest{
				Path: opath,
			}
			_, err = agent.RemoveDir(rmReq)
			if err != nil {
				return fmt.Errorf("remove new origin dir %s failed: %w", opath, err)
			}

			// move the old data back
			req := &pb.MoveDirRequest{
				SrcPath: bpath,
				DstPath: opath,
			}
			_, err = agent.MoveDir(req)
			if err != nil {
				return fmt.Errorf("move data dir back from %s to %s failed: %w", opath, bpath, err)
			}

			logger.WithField("origin path", opath).
				WithField("backup path", bpath).
				Infof("Moveback origin %s data path successfully", s.GetRole().String())
		}
	}

	return nil
}

func (f *Fix) getDead() ([]*meta.ServiceInfo, error) {
	deadServices := make([]*meta.ServiceInfo, 0)

	for host, services := range f.hosts.GetHostServices() {
		logger := log.WithField("host", host)

		// get and check agent
		var agentAddr *nebula.HostAddr
		for _, s := range services {
			if s.GetRole() == meta.HostRole_AGENT {
				if agentAddr == nil {
					agentAddr = s.GetAddr()
				} else {
					return deadServices, fmt.Errorf("there are two agents in host %s: %s, %s", s.GetAddr().GetHost(),
						utils.StringifyAddr(agentAddr), utils.StringifyAddr(s.GetAddr()))
				}
			}
		}
		agent, err := f.agentMgr.GetAgent(agentAddr)
		if err != nil {
			return deadServices, fmt.Errorf("get agent %s failed: %w", utils.StringifyAddr(agentAddr), err)
		}

		// collect all dead services
		for _, s := range services {
			if s.GetRole() == meta.HostRole_AGENT {
				continue
			}

			req := &pb.ServiceStatusRequest{
				Role: utils.ToRole(s.GetRole()),
				Dir:  string(s.GetDir().GetRoot()),
			}

			resp, err := agent.ServiceStatus(req)
			if err != nil {
				return deadServices, fmt.Errorf("get service status in host %s failed: %w", agentAddr.Host, err)
			}

			if resp.Status != pb.Status_RUNNING {
				logger.WithField("dir", req.Dir).WithField("role", s.GetRole().String()).Debugf("%s:%s is dead.",
					s.Role.String(), utils.StringifyAddr(s.Addr))
				deadServices = append(deadServices, s)
			}
		}
	}

	return deadServices, nil
}

func (f *Fix) startDead(deadServices []*meta.ServiceInfo) error {
	for _, ds := range deadServices {
		name := fmt.Sprintf("%s[%s]", ds.GetRole().String(), utils.StringifyAddr(ds.GetAddr()))
		agent, err := f.agentMgr.GetAgentFor(ds.GetAddr())
		if err != nil {
			return fmt.Errorf("get agent for %s failed: %w", name, err)
		}

		req := &pb.StartServiceRequest{
			Role: utils.ToRole(ds.GetRole()),
			Dir:  string(ds.GetDir().GetRoot()),
		}
		_, err = agent.StartService(req)
		if err != nil {
			return fmt.Errorf("start %s by agent failed: %w", name, err)
		}
		log.WithField("addr", utils.StringifyAddr(ds.GetAddr())).
			Infof("Start %s by agent successfully.", name)
	}
	return nil
}

func retry(action func() error, aname string, times int) (err error) {
	for try := 1; try <= times; try++ {
		err = action()
		if err == nil {
			return
		}

		log.WithError(err).Infof("%s failed, try times=%d.", aname, try)
		time.Sleep(time.Second * time.Duration(try))
	}

	return
}

func (f *Fix) Fix() error {
	tryTimes := 3

	// check if all services alive
	allAlive := false
	checkAlive := func() error {
		if ds, err := f.getDead(); err != nil {
			return err
		} else {
			allAlive = len(ds) == 0
			return nil
		}
	}
	err := retry(checkAlive, "Get dead services", tryTimes)
	if allAlive {
		log.Info("All services are OK.")
		return nil
	}
	if err != nil {
		return err
	}

	// stop all service for data movement
	if err := retry(f.r.stopCluster, "Stop all services", tryTimes); err != nil {
		return err
	}

	// move back data path
	if err := retry(f.fixData, "Fix data", tryTimes); err != nil {
		return err
	}

	// start all services
	getdeadThenStart := func() error {
		ds, err := f.getDead()
		if err != nil {
			return fmt.Errorf("get services failed:  %w", err)
		}
		err = f.startDead(ds)
		if err != nil {
			return fmt.Errorf("start dead services failed: %w", err)
		}
		return nil
	}
	if err := retry(getdeadThenStart, "Get dead services then start", tryTimes); err != nil {
		return err
	}

	return nil
}
