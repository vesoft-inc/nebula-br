package metaclient

import (
	"fmt"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
	"go.uber.org/zap"
)

type MetaClient struct {
	client *meta.MetaServiceClient
	log    *zap.Logger
}

var defaultTimeout time.Duration = 120 * time.Second

func NewMetaClient(log *zap.Logger) *MetaClient {
	return &MetaClient{log: log}
}

func (m *MetaClient) RestoreMeta(req *meta.RestoreMetaReq) (*meta.ExecResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.RestoreMeta(req)
}

func (m *MetaClient) CreateBackup(req *meta.CreateBackupReq) (*meta.CreateBackupResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.CreateBackup(req)
}

func (m *MetaClient) DropBackup(req *meta.DropSnapshotReq) (*meta.ExecResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.DropSnapshot(req)
}

func (m *MetaClient) ListCluster(req *meta.ListClusterInfoReq) (*meta.ListClusterInfoResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.ListCluster(req)
}

func (m *MetaClient) ListMetaDir(req *meta.GetMetaDirInfoReq) (*meta.GetMetaDirInfoResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.GetMetaDirInfo(req)
}

func (m *MetaClient) DropSpace(req *meta.DropSpaceReq) (*meta.ExecResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.DropSpace(req)
}

func (m *MetaClient) GetSpaceInfo(req *meta.GetSpaceReq) (*meta.GetSpaceResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.GetSpace(req)
}

func (m *MetaClient) Open(addr string) error {
	m.log.Info("start open meta", zap.String("addr", addr))

	if m.client != nil {
		if err := m.client.CC.Close(); err != nil {
			m.log.Warn("close backup falied", zap.Error(err))
		}
	}

	timeoutOption := thrift.SocketTimeout(defaultTimeout)
	addressOption := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		m.log.Error("open socket failed", zap.Error(err), zap.String("addr", addr))
		return err
	}

	bufferedTranFactory := thrift.NewBufferedTransportFactory(128 << 10)
	transport := thrift.NewFramedTransport(bufferedTranFactory.GetTransport(sock))

	pf := thrift.NewBinaryProtocolFactoryDefault()
	client := meta.NewMetaServiceClientFactory(transport, pf)
	if err := client.CC.Open(); err != nil {
		m.log.Error("open meta failed", zap.Error(err), zap.String("addr", addr))
		return err
	}
	m.client = client
	return nil
}

func (m *MetaClient) Close() error {
	if m.client != nil {
		if err := m.client.CC.Close(); err != nil {
			return err
		}
	}
	return nil
}
