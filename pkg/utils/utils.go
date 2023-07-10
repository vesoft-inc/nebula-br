package utils

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"

	pb "github.com/vesoft-inc/nebula-agent/pkg/proto"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

func DumpMetaToFile(meta *meta.BackupMeta, filename string) error {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open file %s failed: %w", filename, err)
	}
	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)
	defer trans.Close()

	transF := thrift.NewFramedTransport(trans)
	defer transF.Close()

	binaryOut := thrift.NewBinaryProtocol(transF, false, true)

	err = meta.Write(binaryOut)
	if err != nil {
		return fmt.Errorf("write backup meta to %s failed: %w", filename, err)
	}

	err = binaryOut.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush binary out: %w", err)
	}

	return nil
}

func ParseMetaFromFile(filename string) (*meta.BackupMeta, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file %s failed: %w", filename, err)
	}
	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)
	defer trans.Close()

	transF := thrift.NewFramedTransport(trans)
	defer transF.Close()

	binaryIn := thrift.NewBinaryProtocol(transF, false, true)
	m := meta.NewBackupMeta()
	err = m.Read(binaryIn)
	if err != nil {
		return nil, fmt.Errorf("read from backup meta: %s failed: %w", filename, err)
	}
	return m, nil
}

const (
	LocalTmpDir = "/tmp/nebula-br"
)

func EnsureDir(dir string) error {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("ensure dirs %s failed: %w", dir, err)
	}
	return nil
}

func RemoveDir(dir string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return fmt.Errorf("remove tmp dirs %s failed: %w", dir, err)
	}
	return nil
}

func IsBackupName(path string) bool {
	return strings.HasPrefix(path, "BACKUP")
}

func UriJoin(elem ...string) (string, error) {
	if len(elem) == 0 {
		return "", fmt.Errorf("empty paths")
	}

	if len(elem) == 1 {
		return elem[0], nil
	}

	u, err := url.Parse(elem[0])
	if err != nil {
		return "", fmt.Errorf("parse base uri %s failed: %w", elem[0], err)
	}

	elem[0] = u.Path
	u.Path = path.Join(elem...)
	return u.String(), nil
}

func ToRole(r meta.HostRole) pb.ServiceRole {
	switch r {
	case meta.HostRole_STORAGE:
		return pb.ServiceRole_STORAGE
	case meta.HostRole_GRAPH:
		return pb.ServiceRole_GRAPH
	case meta.HostRole_META:
		return pb.ServiceRole_META
	default:
		return pb.ServiceRole_UNKNOWN_ROLE
	}
}

func IsNotExist(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not exist")
}
