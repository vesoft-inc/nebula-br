package log

import "go.uber.org/zap"

type Logger struct {
	path string
	*zap.Logger
}

func NewLogger(logPath string) (*Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{
		logPath,
	}
	log, err := cfg.Build()
	return &Logger{logPath, log}, err
}
