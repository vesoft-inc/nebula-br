package clients

import (
	"context"
	"fmt"

	agent "github.com/vesoft-inc/nebula-agent/pkg/client"
	"github.com/vesoft-inc/nebula-br/pkg/utils"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
)

type NebulaAgent struct {
	agent.Client
}

func NewAgent(ctx context.Context, agentAddr *nebula.HostAddr) (*NebulaAgent, error) {
	cfg := &agent.Config{
		Addr: agentAddr,
	}
	c, err := agent.New(ctx, cfg)
	if err != nil {
		return nil, err
	}

	a := &NebulaAgent{
		Client: c,
	}

	return a, nil
}

type AgentManager struct {
	ctx    context.Context
	agents map[string]*NebulaAgent // group by ip or host
	hosts  *utils.NebulaHosts
}

func NewAgentManager(ctx context.Context, hosts *utils.NebulaHosts) *AgentManager {
	return &AgentManager{
		ctx:    ctx,
		agents: make(map[string]*NebulaAgent),
		hosts:  hosts,
	}
}

func (a *AgentManager) GetAgentFor(serviceAddr *nebula.HostAddr) (*NebulaAgent, error) {
	agentAddr, err := a.hosts.GetAgentFor(serviceAddr)
	if err != nil {
		return nil, fmt.Errorf("get agent address for graph service %s failed: %w",
			utils.StringifyAddr(agentAddr), err)
	}

	return a.GetAgent(agentAddr)
}

func (a *AgentManager) GetAgent(agentAddr *nebula.HostAddr) (*NebulaAgent, error) {
	if agent, ok := a.agents[agentAddr.Host]; ok {
		if agent.GetAddr().Host != agentAddr.Host || agent.GetAddr().Port != agentAddr.Port {
			return nil, fmt.Errorf("there are two agents, %s and %s, in the same host: %s",
				utils.StringifyAddr(agent.GetAddr()), utils.StringifyAddr(agentAddr), agentAddr.Host)
		}

		return agent, nil
	}

	agent, err := NewAgent(a.ctx, agentAddr)
	if err != nil {
		return nil, fmt.Errorf("create agent %s failed: %w", utils.StringifyAddr(agentAddr), err)
	}

	a.agents[agentAddr.Host] = agent
	return agent, nil
}
