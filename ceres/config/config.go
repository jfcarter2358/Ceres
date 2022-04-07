// config.go

package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type ConfigObject struct {
	LogLevel         string `json:"log-level" binding:"required"`
	CeresDir         string `json:"ceres-dir" binding:"required"`
	DataDir          string `json:"data-dir" binding:"required"`
	IndexDir         string `json:"index-dir" binding:"required"`
	StorageLineLimit int    `json:"storage-line-limit" binding:"required"`
	Port             int    `json:"port" binding:"required"`
}

var Config ConfigObject

func ReadConfigFile() *ConfigObject {

	// Set default path if we are not passed one
	path := os.Getenv("CERES_CONFIG")
	if path == "" {
		path = ".ceres/config/config.json"
	}

	// Open our jsonFile
	jsonFile, err := os.Open(path)

	// If os.Open returns an error then handle it
	if err != nil {
		panic(err)
	}

	// Read and unmarshal the JSON
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &Config)

	defer jsonFile.Close()

	return &Config
}
