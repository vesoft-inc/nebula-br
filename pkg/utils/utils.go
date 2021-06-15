package utils

import (
	"os"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
	"go.uber.org/zap"
)

func PutMetaToFile(logger *zap.Logger, meta *meta.BackupMeta, filename string) error {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logger.Error("store backupmeta failed in open file",
			zap.String("filename", filename),
			zap.String("error", err.Error()))
		return err
	}
	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)
	defer trans.Close()

	transF := thrift.NewFramedTransport(trans)
	defer transF.Close()

	binaryOut := thrift.NewBinaryProtocol(transF, false, true)

	err = meta.Write(binaryOut)
	if err != nil {
		logger.Error("store backupmeta failed in write",
			zap.String("filename", filename),
			zap.String("error", err.Error()))
		return err
	}

	binaryOut.Flush()
	return nil
}

func GetMetaFromFile(logger *zap.Logger, filename string) (*meta.BackupMeta, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		logger.Error("get backupmeta failed in open file",
			zap.String("filename", filename),
			zap.String("error", err.Error()))
		return nil, err
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
		logger.Error("get backupmeta failed in read", zap.String("filename", filename), zap.String("error", err.Error()))
		return nil, err
	}
	return m, nil
}
