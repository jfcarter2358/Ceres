package config

import (
	"os"
	"testing"
)

func TestReadConfigFile(t *testing.T) {
	// Expected values to check against
	expectedLogLevel := "debug"
	expectedCeresDir := "../../test/.ceresdb"
	expectedDataDir := "../../test/.ceresdb/data"
	expectedStorageLineLimit := 32

	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb/config/config.json")
	conf := ReadConfigFile()

	if conf.LogLevel != expectedLogLevel {
		t.Errorf("LogLevel was incorrect, got: %s, want: %s", conf.LogLevel, expectedLogLevel)
	}
	if conf.HomeDir != expectedCeresDir {
		t.Errorf("CeresDir was incorrect, got: %s, want: %s", conf.HomeDir, expectedCeresDir)
	}
	if conf.DataDir != expectedDataDir {
		t.Errorf("DataDir was incorrect, got: %s, want: %s", conf.DataDir, expectedDataDir)
	}
	if conf.StorageLineLimit != expectedStorageLineLimit {
		t.Errorf("StorageLineLimit was incorrect, got: %d, want: %d", conf.StorageLineLimit, expectedStorageLineLimit)
	}
}

func TestReadConfigFilePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic with an invalid config path")
		}
	}()

	os.Setenv("CERESDB_CONFIG_PATH", "")
	ReadConfigFile()
}

func TestReadConfigEnv(t *testing.T) {
	// Expected values to check against
	expectedLogLevel := "info"
	expectedCeresDir := ".ceresdb"
	expectedDataDir := ".ceresdb/data"
	expectedStorageLineLimit := 64

	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb/config/config.json")
	os.Setenv("CERESDB_LOG_LEVEL", "info")
	os.Setenv("CERESDB_HOME_DIR", ".ceresdb")
	os.Setenv("CERESDB_DATA_DIR", ".ceresdb/data")
	os.Setenv("CERESDB_INDEX_DIR", ".ceresdb/indices")
	os.Setenv("CERESDB_STORAGE_LINE_LIMIT", "64")
	os.Setenv("CERESDB_PORT", "1234")
	conf := ReadConfigFile()

	if conf.LogLevel != expectedLogLevel {
		t.Errorf("LogLevel was incorrect, got: %s, want: %s", conf.LogLevel, expectedLogLevel)
	}
	if conf.HomeDir != expectedCeresDir {
		t.Errorf("CeresDir was incorrect, got: %s, want: %s", conf.HomeDir, expectedCeresDir)
	}
	if conf.DataDir != expectedDataDir {
		t.Errorf("DataDir was incorrect, got: %s, want: %s", conf.DataDir, expectedDataDir)
	}
	if conf.StorageLineLimit != expectedStorageLineLimit {
		t.Errorf("StorageLineLimit was incorrect, got: %d, want: %d", conf.StorageLineLimit, expectedStorageLineLimit)
	}
}
