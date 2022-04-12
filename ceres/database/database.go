package database

import (
	"ceres/collection"
	"ceres/config"
	"ceres/freespace"
	"ceres/record"
	"ceres/schema"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
)

func Delete(database string) error {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	if err := os.RemoveAll(dataPath); err != nil {
		return err
	}
	if err := os.RemoveAll(indexPath); err != nil {
		return err
	}
	delete(freespace.FreeSpace.Databases, database)
	delete(schema.Schema.Databases, database)
	return nil
}

func Get() ([]map[string]interface{}, error) {
	var databases []map[string]interface{}
	dirInfo, err := ioutil.ReadDir(config.Config.DataDir)
	if err != nil {
		return databases, err
	}

	for _, dir := range dirInfo {
		databases = append(databases, map[string]interface{}{"name": dir.Name()})
	}
	return databases, nil
}

func Patch() error {
	return errors.New("PATCH action is unsupported on resource DATABASE")
}

func Post(database string) error {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return err
	}
	freespace.FreeSpace.Databases[database] = freespace.FreeSpaceDatabase{}
	schema.Schema.Databases[database] = schema.SchemaDatabase{}
	freespace.WriteFreeSpace()
	schema.WriteSchema()
	if database != "_auth" {
		collection.Post(database, "_users", map[string]interface{}{"username": "STRING", "role": "STRING"})
		inputData := []map[string]interface{}{{"username": "ceres", "role": "ADMIN"}}
		err := record.Post(database, "_users", inputData)
		if err != nil {
			return err
		}
	}
	return nil
}

func Put() error {
	return errors.New("PUT action is unsupported on resource DATABASE")
}
