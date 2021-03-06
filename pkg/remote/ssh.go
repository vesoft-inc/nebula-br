package remote

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"github.com/vesoft-inc/nebula-br/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	client *ssh.Client
	addr   string
	user   string
	log    *zap.Logger
}

func NewClient(addr string, user string, log *zap.Logger) (*Client, error) {
	key, err := ioutil.ReadFile(os.Getenv("HOME") + "/.ssh/id_rsa")
	if err != nil {
		log.Error("unable to read private key", zap.Error(err))
		return nil, err
	}

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Error("unable to parse private key", zap.Error(err))
		return nil, err
	}
	config := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
	}

	retry := 0
	for retry < 3 {
		var client *ssh.Client
		client, err = ssh.Dial("tcp", net.JoinHostPort(addr, "22"), config)
		if err != nil {
			log.Error("unable to connect host, will retry", zap.Int("attemp", retry), zap.Error(err), zap.String("host", addr), zap.String("user", user))
			time.Sleep(time.Second * 1)
			retry += 1
			continue
		}
		return &Client{client, addr, user, log}, nil

	}
	return nil, err
}

func NewClientPool(addr string, user string, log *zap.Logger, count int) ([]*Client, error) {
	var clients []*Client
	for i := 0; i < count; i++ {
		client, err := NewClient(addr, user, log)
		if err != nil {
			for _, c := range clients {
				c.Close()
			}
			return nil, err
		}
		clients = append(clients, client)
	}

	return clients, nil
}

func GetAddresstoReachRemote(addr string, user string, log *zap.Logger) (string, error) {
	if cli, err := NewClient(addr, user, log); err == nil {
		log.Info("succeed to reach remote", zap.String("addr of local", cli.client.Conn.LocalAddr().String()))
		return strings.Split(cli.client.Conn.LocalAddr().String(), ":")[0], nil
	} else {
		return "", err
	}
}

func (c *Client) Close() {
	c.client.Close()
}

func (c *Client) newSession() (*ssh.Session, error) {
	session, err := c.client.NewSession()
	if err != nil {
		c.log.Error("new session failed", zap.Error(err))
		return nil, err
	}
	return session, nil
}

func (c *Client) ExecCommandBySSH(cmd string) error {
	session, err := c.newSession()
	if err != nil {
		return err
	}
	defer session.Close()
	c.log.Info("ssh will exec", zap.String("addr", c.addr), zap.String("cmd", cmd), zap.String("user", c.user))
	var stdoutBuf bytes.Buffer
	session.Stdout = &stdoutBuf

	err = session.Run(cmd)
	if err != nil {
		c.log.Error("ssh run failed", zap.Error(err), zap.String("addr", c.addr), zap.String("cmd", cmd))
		return err
	}
	c.log.Info("Command execution completed", zap.String("addr", c.addr), zap.String("cmd", cmd))
	return nil
}

func CheckCommand(checkCommand string, nodes []config.NodeInfo, log *zap.Logger) error {
	g, _ := errgroup.WithContext(context.Background())
	for _, node := range nodes {
		addr := node.Addrs
		ipAddrs := strings.Split(addr, ":")
		user := node.User
		client, err := NewClient(ipAddrs[0], user, log)
		if err != nil {
			return err
		}
		g.Go(func() error { return client.ExecCommandBySSH(checkCommand) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}
