// config.go

package config

import (
	"ceresdb/logger"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

const DEFAULT_CONFIG_PATH = ".ceresdb/config/config.json"
const ENV_PREFIX = "CERESDB_"

type ConfigObject struct {
	LogLevel          string `json:"log_level" env:"LOG_LEVEL"`
	LogFormat         string `json:"log_format" env:"LOG_FORMAT"`
	DataDir           string `json:"data_dir" env:"DATA_DIR"`
	StorageLineLimit  int    `json:"storage_line_limit" env:"STORAGE_LINE_LIMIT"`
	Port              int    `json:"port" env:"PORT"`
	AdminUsername     string `json:"admin_username" env:"ADMIN_USERNAME"`
	AdminPassword     string `json:"admin_password" env:"ADMIN_PASSWORD"`
	MaxIndexSize      int    `json:"max_index_size" env:"MAX_INDEX_SIZE"`
	RetentionPeriod   int    `json:"retention_period" env:"RETENTION_PERIOD"`
	RetentionInterval int    `json:"retention_interval" env:"RETENTION_INTERVAL"`
}

var Config ConfigObject

func ReadConfig() {
	configPath := os.Getenv(ENV_PREFIX + "CONFIG_PATH")
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	Config = ConfigObject{
		LogLevel:          logger.LOG_LEVEL_INFO,
		LogFormat:         logger.LOG_FORMAT_CONSOLE,
		DataDir:           "/tmp/ceresdb",
		StorageLineLimit:  1000,
		Port:              7437,
		AdminUsername:     "ceresdb",
		AdminPassword:     "ceresdb",
		MaxIndexSize:      1000,
		RetentionPeriod:   -1,
		RetentionInterval: 60,
	}

	jsonFile, err := os.Open(configPath)
	if err == nil {
		log.Printf("Successfully Opened %v", configPath)

		byteValue, _ := ioutil.ReadAll(jsonFile)

		json.Unmarshal(byteValue, &Config)
	}

	v := reflect.ValueOf(Config)
	t := reflect.TypeOf(Config)

	for i := 0; i < v.NumField(); i++ {
		field, found := t.FieldByName(v.Type().Field(i).Name)
		if !found {
			continue
		}

		value := field.Tag.Get("env")
		if value != "" {
			val, present := os.LookupEnv(ENV_PREFIX + value)
			if present {
				// log.Printf("Found ENV var %s with value %s", ENV_PREFIX+value, val)
				w := reflect.ValueOf(&Config).Elem().FieldByName(t.Field(i).Name)
				x := getAttr(&Config, t.Field(i).Name).Kind().String()
				if w.IsValid() {
					switch x {
					case "int", "int64":
						i, err := strconv.ParseInt(val, 10, 64)
						if err == nil {
							w.SetInt(i)
						}
					case "int8":
						i, err := strconv.ParseInt(val, 10, 8)
						if err == nil {
							w.SetInt(i)
						}
					case "int16":
						i, err := strconv.ParseInt(val, 10, 16)
						if err == nil {
							w.SetInt(i)
						}
					case "int32":
						i, err := strconv.ParseInt(val, 10, 32)
						if err == nil {
							w.SetInt(i)
						}
					case "string":
						w.SetString(val)
					case "float32":
						i, err := strconv.ParseFloat(val, 32)
						if err == nil {
							w.SetFloat(i)
						}
					case "float", "float64":
						i, err := strconv.ParseFloat(val, 64)
						if err == nil {
							w.SetFloat(i)
						}
					case "bool":
						i, err := strconv.ParseBool(val)
						if err == nil {
							w.SetBool(i)
						}
					default:
						objValue := reflect.New(field.Type)
						objInterface := objValue.Interface()
						err := json.Unmarshal([]byte(val), objInterface)
						obj := reflect.ValueOf(objInterface)
						if err == nil {
							w.Set(reflect.Indirect(obj).Convert(field.Type))
						} else {
							log.Println(err)
						}
					}
				}
			}
		}
	}

	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	usr, _ := user.Current()
	dir := usr.HomeDir

	// Expand home directory
	if Config.DataDir == "~" {
		Config.DataDir = dir
	} else if strings.HasPrefix(Config.DataDir, "~/") {
		Config.DataDir = filepath.Join(dir, Config.DataDir[2:])
	}
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
