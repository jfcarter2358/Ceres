package config

import (
	"ceresdb/logger"
	"fmt"
	"os"
	"testing"
)

var Expected = []ConfigObject{
	{
		LogLevel:         logger.LOG_LEVEL_INFO,
		DataDir:          "/tmp/ceresdb",
		StorageLineLimit: 1000,
		Port:             7437,
		AdminUsername:    "ceresdb",
		AdminPassword:    "ceresdb",
	},
	{
		LogLevel:         logger.LOG_LEVEL_DEBUG,
		DataDir:          "/tmp/ceresdb2",
		StorageLineLimit: 2000,
		Port:             7438,
		AdminUsername:    "foo",
		AdminPassword:    "bar",
	},
}

func setConfigEnv(conf ConfigObject) {
	os.Setenv("CERESDB_LOG_LEVEL", conf.LogLevel)
	os.Setenv("CERESDB_DATA_DIR", conf.DataDir)
	os.Setenv("CERESDB_STORAGE_LINE_LIMIT", fmt.Sprintf("%d", conf.StorageLineLimit))
	os.Setenv("CERESDB_PORT", fmt.Sprintf("%d", conf.Port))
	os.Setenv("CERESDB_ADMIN_USERNAME", conf.AdminUsername)
	os.Setenv("CERESDB_ADMIN_PASSWORD", conf.AdminPassword)
}

func checkConfig(conf ConfigObject) error {
	if conf.LogLevel != Config.LogLevel {
		return fmt.Errorf("log level was incorrect, got: %s, want: %s", conf.LogLevel, Config.LogLevel)
	}
	if conf.DataDir != Config.DataDir {
		return fmt.Errorf("data dir was incorrect, got: %s, want: %s", Config.DataDir, conf.DataDir)
	}
	if conf.StorageLineLimit != Config.StorageLineLimit {
		return fmt.Errorf("storage line limit was incorrect, got: %d, want: %d", Config.StorageLineLimit, conf.StorageLineLimit)
	}
	if conf.Port != Config.Port {
		return fmt.Errorf("port was incorrect, got: %d, want: %d", Config.Port, conf.Port)
	}
	if conf.AdminUsername != Config.AdminUsername {
		return fmt.Errorf("admin username was incorrect, got: %s, want: %s", Config.AdminUsername, conf.AdminUsername)
	}
	if conf.AdminPassword != Config.AdminPassword {
		return fmt.Errorf("admin password was incorrect, got: %s, want: %s", Config.AdminPassword, conf.AdminPassword)
	}
	return nil
}

func TestReadConfig(t *testing.T) {
	ReadConfig()
	if err := checkConfig(Expected[0]); err != nil {
		t.Errorf(err.Error())
	}

	for _, conf := range Expected[1:] {
		setConfigEnv(conf)
		ReadConfig()
		if err := checkConfig(conf); err != nil {
			t.Error(err.Error())
		}
	}
}
