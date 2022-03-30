package config

import (
	"os"
	"testing"
)

func TestReadConfigFile(t *testing.T) {
	// Expected values to check against
	expectedLogLevel := "debug"
	expectedCeresDir := "../../test/.ceres"
	expectedDataDir := "../../test/.ceres/data"
	expectedStorageLineLimit := 32

	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	conf := ReadConfigFile()

	if conf.LogLevel != expectedLogLevel {
		t.Errorf("LogLevel was incorrect, got: %s, want: %s", conf.LogLevel, expectedLogLevel)
	}
	if conf.CeresDir != expectedCeresDir {
		t.Errorf("CeresDir was incorrect, got: %s, want: %s", conf.CeresDir, expectedCeresDir)
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

	os.Setenv("CERES_CONFIG", "")
	ReadConfigFile()
}
