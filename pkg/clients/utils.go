package clients

import (
	"fmt"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"

	"github.com/vesoft-inc/nebula-br/pkg/utils"
)

const (
	defaultTimeout = 120 * time.Second
)

func connect(metaAddr *nebula.HostAddr) (*meta.MetaServiceClient, error) {
	log.WithField("meta address", utils.StringifyAddr(metaAddr)).Info("try to connect meta service")
	timeoutOption := thrift.SocketTimeout(defaultTimeout)
	addressOption := thrift.SocketAddr(utils.StringifyAddr(metaAddr))
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return nil, fmt.Errorf("open socket failed: %w", err)
	}

	bufferedTranFactory := thrift.NewBufferedTransportFactory(128 << 10)
	transport := thrift.NewFramedTransport(bufferedTranFactory.GetTransport(sock))
	pf := thrift.NewBinaryProtocolFactoryDefault()
	client := meta.NewMetaServiceClientFactory(transport, pf)
	if err := client.CC.Open(); err != nil {
		return nil, fmt.Errorf("open meta failed %w", err)
	}

	log.WithField("meta address", utils.StringifyAddr(metaAddr)).Info("connect meta server successfully")
	return client, nil
}
