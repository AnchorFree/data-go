package logger

import (
	"fmt"
	"github.com/imdario/mergo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.SugaredLogger

type Props struct {
	LogLevel  string
	LogFormat string
}

var DefaultConfig Props = Props{
	LogLevel:  "info",
	LogFormat: "json",
}

func Init(cfg Props) {
	if err := mergo.Merge(&cfg, DefaultConfig); err != nil {
		panic("Could not merge config")
	}
	l := zap.NewAtomicLevel()
	err := l.UnmarshalText([]byte(cfg.LogLevel))
	if err != nil {
		panic(err)
	}
	zCfg := zap.Config{
		Encoding:         cfg.LogFormat,
		Level:            l,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:   "msg",
			LevelKey:     "level",
			EncodeLevel:  zapcore.CapitalLevelEncoder,
			TimeKey:      "time",
			EncodeTime:   zapcore.ISO8601TimeEncoder,
			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	z, err := zCfg.Build()
	if err != nil {
		fmt.Println(err)
	}
	logger = z.Sugar()
}

func Get() *zap.SugaredLogger {
	if logger == nil {
		Init(Props{})
	}
	return logger
}
