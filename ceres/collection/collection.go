package collection

import (
	"ceres/config"
	"ceres/freespace"
	"ceres/schema"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
)

func Delete(database, collection string) error {
	dataPath := filepath.Join(config.Config.DataDir, database, collection)
	indexPath := filepath.Join(config.Config.IndexDir, database, collection)
	if err := os.RemoveAll(dataPath); err != nil {
		return err
	}
	if err := os.RemoveAll(indexPath); err != nil {
		return err
	}
	freespaceDB := freespace.FreeSpace.Databases[database]
	delete(freespaceDB.Collections, collection)
	freespace.FreeSpace.Databases[database] = freespaceDB
	schemaDB := schema.Schema.Databases[database]
	delete(schemaDB.Collections, collection)
	schema.Schema.Databases[database] = schemaDB
	freespace.WriteFreeSpace()
	schema.WriteSchema()
	return nil
}

func Get(database string) ([]map[string]interface{}, error) {
	var collections []map[string]interface{}
	dirInfo, err := ioutil.ReadDir(filepath.Join(config.Config.DataDir, database))
	if err != nil {
		return collections, err
	}

	for _, dir := range dirInfo {
		collections = append(collections, map[string]interface{}{"name": dir.Name(), "schema": schema.Schema.Databases[database].Collections[dir.Name()].Types})
	}
	return collections, nil
}

func Patch() error {
	return errors.New("PATCH action is unsupported on resource COLLECTION")
}

func Post(database, collection string, newSchema map[string]interface{}) error {
	schemaTypes := make(map[string]string)
	for k, v := range newSchema {
		schemaTypes[k] = v.(string)
	}
	if err := schema.ValidateSchemaCollection(schemaTypes); err != nil {
		return err
	}
	dataPath := filepath.Join(config.Config.DataDir, database, collection)
	indexPath := filepath.Join(config.Config.IndexDir, database, collection)
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return err
	}
	allPath := filepath.Join(config.Config.IndexDir, database, collection, "all")
	os.WriteFile(allPath, []byte(""), 0644)
	freespaceDB := freespace.FreeSpace.Databases[database]
	if freespaceDB.Collections == nil {
		freespaceDB.Collections = make(map[string]freespace.FreeSpaceCollection)
	}
	freespaceDB.Collections[collection] = freespace.FreeSpaceCollection{}
	freespace.FreeSpace.Databases[database] = freespaceDB
	schemaDB := schema.Schema.Databases[database]
	schemaCol := schema.SchemaCollection{}
	if schemaDB.Collections == nil {
		schemaDB.Collections = make(map[string]schema.SchemaCollection)
	}
	if val, ok := schemaDB.Collections[collection]; ok {
		schemaCol = val
	}
	schemaCol.Types = schemaTypes
	schemaDB.Collections[collection] = schemaCol
	schema.Schema.Databases[database] = schemaDB
	freespace.WriteFreeSpace()
	schema.WriteSchema()
	return nil
}

func Put(database, collection string, newSchema map[string]interface{}) error {
	schemaDB := schema.Schema.Databases[database]
	schemaCol := schema.SchemaCollection{}
	schemaTypes := make(map[string]string)
	if schemaDB.Collections == nil {
		schemaDB.Collections = make(map[string]schema.SchemaCollection)
	}
	for k, v := range newSchema {
		schemaTypes[k] = v.(string)
	}
	if err := schema.ValidateSchemaCollection(schemaTypes); err != nil {
		return err
	}
	schemaCol.Types = schemaTypes
	schemaDB.Collections[collection] = schemaCol
	schema.Schema.Databases[database] = schemaDB
	schema.WriteSchema()
	return nil
}
