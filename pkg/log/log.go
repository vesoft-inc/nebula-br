package log

import (
	"io"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/vesoft-inc/nebula-br/pkg/config"
)

func SetLog(flags *pflag.FlagSet) error {
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000Z",
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			s := strings.Split(f.Function, ".")
			funcname := s[len(s)-1]
			_, filename := path.Split(f.File)
			return funcname, filename
		},
	})

	debug, err := flags.GetBool(config.FlagLogDebug)
	if err != nil {
		return err
	}
	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}

	path, err := flags.GetString(config.FlagLogPath)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logrus.WithError(err).WithField("file", path).Error("Create log path failed.")
		return err
	}

	mw := io.MultiWriter(os.Stdout, file)
	logrus.SetOutput(mw)

	return nil
}
