package mr

import (
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Logger *zap.Logger

type constantClock time.Time

func (c constantClock) Now() time.Time { return time.Time(c) }
func (c constantClock) NewTicker(d time.Duration) *time.Ticker {
	return &time.Ticker{}
}

func InitLogger(logLevel string, logpath string) {

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder   // UTC时间
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // 日志等级大写

	lumberJackLogger := &lumberjack.Logger{
		Filename:   logpath,
		MaxSize:    100,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
	}

	// 日志等级，默认INFO
	logLevel = strings.ToUpper(logLevel)
	var levelEnabler zapcore.LevelEnabler
	switch logLevel {
	case "DEBUG":
		levelEnabler = zap.DebugLevel
	case "INFO":
		levelEnabler = zap.InfoLevel
	case "WARN":
		levelEnabler = zap.WarnLevel
	case "ERROR":
		levelEnabler = zap.ErrorLevel
	default:
		levelEnabler = zap.InfoLevel
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(lumberJackLogger),
		levelEnabler,
	)

	Logger = zap.New(core, zap.AddCaller(), zap.Development())

	Logger = Logger.WithOptions(zap.WithClock(constantClock(time.Now())))

	Logger.Info("Log module init success")
}

// func Any(key string, value interface{}) zap.Field {
// 	return zap.Any(key, value)
// }
