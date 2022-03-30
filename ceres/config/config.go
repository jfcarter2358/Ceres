// config.go

package config

import (
	log "ceres/logging"
	"encoding/json"
	"io/ioutil"
	"os"
)

type ConfigObject struct {
	LogLevel         string `json:"log-level" binding:"required"`
	CeresDir         string `json:"ceres-dir" binding:"required"`
	DataDir          string `json:"data-dir" binding:"required"`
	StorageLineLimit int    `json:"storage-line-limit" binding:"required"`
}

var Config ConfigObject

func ReadConfigFile() *ConfigObject {
	log.DEBUG("Loading Ceres configuration...")

	// Set default path if we are not passed one
	path := os.Getenv("CERES_CONFIG")
	if path == "" {
		path = "config/config.json"
	}

	// Open our jsonFile
	jsonFile, err := os.Open(path)

	// If we os.Open returns an error then handle it
	if err != nil {
		log.ERROR("Unable to read json file")
		panic(err)
	}

	log.INFO("Successfully Opened config/config.json")

	// Read and unmarshal the JSON
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &Config)

	defer jsonFile.Close()

	return &Config
}
