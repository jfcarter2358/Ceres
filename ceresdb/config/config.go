// config.go

package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

type ConfigObject struct {
	LogLevel         string `json:"log-level" binding:"required" env:"CERESDB_LOG_LEVEL"`
	HomeDir          string `json:"home-dir" binding:"required" env:"CERESDB_HOME_DIR"`
	DataDir          string `json:"data-dir" binding:"required" env:"CERESDB_DATA_DIR"`
	IndexDir         string `json:"index-dir" binding:"required" env:"CERESDB_INDEX_DIR"`
	StorageLineLimit int    `json:"storage-line-limit" binding:"required" env:"CERESDB_STORAGE_LINE_LIMIT"`
	Port             int    `json:"port" binding:"required" env:"CERESDB_PORT"`
}

var Config ConfigObject

func ReadConfigFile() *ConfigObject {

	// Set default path if we are not passed one
	path := os.Getenv("CERESDB_CONFIG_PATH")
	if path == "" {
		path = ".ceresdb/config/config.json"
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

	v := reflect.ValueOf(Config)
	t := reflect.TypeOf(Config)

	for i := 0; i < v.NumField(); i++ {
		field, found := t.FieldByName(v.Type().Field(i).Name)
		if !found {
			continue
		}

		value := field.Tag.Get("env")
		if value != "" {
			val, present := os.LookupEnv(value)
			if present {
				w := reflect.ValueOf(&Config).Elem().FieldByName(t.Field(i).Name)
				x := getAttr(&Config, t.Field(i).Name).Kind().String()
				if w.IsValid() {
					switch x {
					case "int", "int64":
						i, err := strconv.ParseInt(val, 10, 64)
						if err == nil {
							w.SetInt(i)
						}
					// case "int8":
					// 	i, err := strconv.ParseInt(val, 10, 8)
					// 	if err == nil {
					// 		w.SetInt(i)
					// 	}
					// case "int16":
					// 	i, err := strconv.ParseInt(val, 10, 16)
					// 	if err == nil {
					// 		w.SetInt(i)
					// 	}
					// case "int32":
					// 	i, err := strconv.ParseInt(val, 10, 32)
					// 	if err == nil {
					// 		w.SetInt(i)
					// 	}
					case "string":
						w.SetString(val)
						// case "float32":
						// 	i, err := strconv.ParseFloat(val, 32)
						// 	if err == nil {
						// 		w.SetFloat(i)
						// 	}
						// case "float", "float64":
						// 	i, err := strconv.ParseFloat(val, 64)
						// 	if err == nil {
						// 		w.SetFloat(i)
						// 	}
						// case "bool":
						// 	i, err := strconv.ParseBool(val)
						// 	if err == nil {
						// 		w.SetBool(i)
						// 	}
					}
				}
			}
		}
	}

	defer jsonFile.Close()

	usr, _ := user.Current()
	dir := usr.HomeDir

	// Expand home directory
	if Config.HomeDir == "~" {
		Config.HomeDir = dir
	} else if strings.HasPrefix(Config.HomeDir, "~/") {
		Config.HomeDir = filepath.Join(dir, Config.HomeDir[2:])
	}
	if Config.DataDir == "~" {
		Config.DataDir = dir
	} else if strings.HasPrefix(Config.DataDir, "~/") {
		Config.DataDir = filepath.Join(dir, Config.DataDir[2:])
	}
	if Config.IndexDir == "~" {
		Config.IndexDir = dir
	} else if strings.HasPrefix(Config.IndexDir, "~/") {
		Config.IndexDir = filepath.Join(dir, Config.IndexDir[2:])
	}

	return &Config
}

func getAttr(obj interface{}, fieldName string) reflect.Value {
	pointToStruct := reflect.ValueOf(obj) // addressable
	curStruct := pointToStruct.Elem()
	if curStruct.Kind() != reflect.Struct {
		panic("not struct")
	}
	curField := curStruct.FieldByName(fieldName) // type: reflect.Value
	if !curField.IsValid() {
		panic("not found:" + fieldName)
	}
	return curField
}
