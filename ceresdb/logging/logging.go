// logging.go

package logging

import (
	log "github.com/sirupsen/logrus"
)

func Initialize(level string) {
	log.SetFormatter(&log.JSONFormatter{})
	// log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	lvl, err := log.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	log.SetLevel(lvl)
}

func TRACE(message string) {
	log.Trace(message)
}

func DEBUG(message string) {
	log.Debug(message)
}

func INFO(message string) {
	log.Info(message)
}

func WARN(message string) {
	log.Warn(message)
}

func ERROR(message string) {
	log.Error(message)
}

func FATAL(message string) {
	log.Fatal(message)
}

func PANIC(message string) {
	log.Panic(message)
}
