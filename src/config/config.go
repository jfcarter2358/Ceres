package config

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
)

const DEFAULT_CONFIG_PATH = "/tmp/config.json"
const ENV_PREFIX = "CERESDB_"

type ConfigObject struct {
	Port                 int    `json:"http_port" env:"HTTP_PORT" default:"7437"`
	LogLevel             string `json:"log_level" env:"LOG_LEVEL" default:"INFO"`
	LogFormat            string `json:"log_format" env:"LOG_FORMAT" default:"console"`
	InitialAdminUsername string `json:"initial_admin_username" env:"INITIAL_ADMIN_USERNAME" default:"admin"`
	InitialAdminPassword string `json:"initial_admin_password" env:"INITIAL_ADMIN_PASSWORD" default:"admin"`
}

var Config ConfigObject

func LoadConfig() {
	configPath := os.Getenv(ENV_PREFIX + "CONFIG_PATH")
	if configPath == "" {
		configPath = DEFAULT_CONFIG_PATH
	}

	Config = ConfigObject{}

	loadDefault()
	loadFile(configPath)
	loadEnv()
}

func loadFile(configPath string) {
	log.Printf("Load File Config Before: %v", Config)
	jsonFile, err := os.Open(configPath)
	if err == nil {
		log.Printf("Successfully Opened %v", configPath)

		byteValue, _ := io.ReadAll(jsonFile)

		json.Unmarshal(byteValue, &Config)
	}
	defer jsonFile.Close()
	log.Printf("Load File Config After: %v", Config)
}

func loadDefault() {
	log.Printf("Load Default Config Before: %v", Config)
	v := reflect.ValueOf(Config)
	t := reflect.TypeOf(Config)

	for i := 0; i < v.NumField(); i++ {
		field, found := t.FieldByName(v.Type().Field(i).Name)
		if !found {
			continue
		}

		val := field.Tag.Get("default")
		if val != "" {
			log.Printf("Got default for %s of %s: %v", field.Name, val, Config)
			castType(t, i, val, field)
		}
	}
	log.Printf("Load Default Config After: %v", Config)
}

func loadEnv() {
	log.Printf("Load ENV Config Before: %v", Config)
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
				log.Printf("Got env for %s of %s: %v", field.Name, val, Config)
				castType(t, i, val, field)
			}
		}
	}
	log.Printf("Load ENV Config After: %v", Config)
}

func castType(t reflect.Type, i int, val string, field reflect.StructField) {
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
		case "slice", "map":
			objValue := reflect.New(field.Type)
			objInterface := objValue.Interface()
			err := json.Unmarshal([]byte(val), objInterface)
			obj := reflect.ValueOf(objInterface)
			if err == nil {
				w.Set(reflect.Indirect(obj).Convert(field.Type))
			} else {
				log.Println(err)
			}
		default:
			log.Printf("Load Flag Config After: %v", Config)
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
