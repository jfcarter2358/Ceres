// schema.go

package schema

import (
	"ceresdb/config"
	"ceresdb/utils"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type SchemaCollection struct {
	Types map[string]string
}

type SchemaDatabase struct {
	Collections map[string]SchemaCollection
}

type SchemaStruct struct {
	Databases map[string]SchemaDatabase
}

var Schema SchemaStruct

func LoadSchema() error {
	path := filepath.Join(config.Config.HomeDir, "schema.json")

	// Open our jsonFile
	jsonFile, err := os.Open(path)

	// If os.Open returns an error then handle it
	if err != nil {
		return err
	}

	// Read and unmarshal the JSON
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var f interface{}

	// Read the JSON
	err = json.Unmarshal(byteValue, &f)
	if err != nil {
		return err
	}

	itemsMap := f.(map[string]interface{})
	Schema.Databases = make(map[string]SchemaDatabase)

	// Loop through the top-level items (database names)
	for dbKey, dbVal := range itemsMap {
		dbItem := SchemaDatabase{}
		dbItem.Collections = make(map[string]SchemaCollection)
		dbItemsMap := dbVal.(map[string]interface{})

		// Loop through the 2nd-level items (collection names)
		for colKey, colVal := range dbItemsMap {
			colItem := SchemaCollection{}
			colItem.Types = make(map[string]string)
			colItemsMap := colVal.(map[string]interface{})

			// Loop through the 3rd-level items (types)
			for typeKey, typeVal := range colItemsMap {
				colItem.Types[typeKey] = typeVal.(string)
			}

			dbItem.Collections[colKey] = colItem
		}
		Schema.Databases[dbKey] = dbItem
	}

	return nil
}

func WriteSchema() error {
	path := filepath.Join(config.Config.HomeDir, "schema.json")

	output := make(map[string]interface{})
	for dbKey, db := range Schema.Databases {
		dbInterface := make(map[string]interface{})
		for colKey, col := range db.Collections {
			colInterface := make(map[string]string)
			for typeKey, typeVal := range col.Types {

				colInterface[typeKey] = typeVal
			}
			dbInterface[colKey] = colInterface
		}
		output[dbKey] = dbInterface
	}

	freeSpaceContents, _ := json.MarshalIndent(output, "", "    ")
	_ = ioutil.WriteFile(path, freeSpaceContents, 0644)

	return nil
}

func ValidateSchemaCollection(schemaCollection map[string]string) error {
	validTypes := []string{"INT", "BOOL", "FLOAT", "STRING", "DICT", "LIST", "ANY"}
	for _, val := range schemaCollection {
		if !utils.Contains(validTypes, val) {
			return errors.New(fmt.Sprintf("Invalid schema type: %v, valid types are 'INT', 'BOOL', 'FLOAT', 'STRING', 'DICT', 'LIST', or 'ANY'", val))
		}
	}
	return nil
}

func Get(database, collection string) map[string]string {
	return Schema.Databases[database].Collections[collection].Types
}

func ValidateDataAgainstSchema(database, collection string, data []map[string]interface{}) error {
	schemaCollection := Schema.Databases[database].Collections[collection]
	for idx, datum := range data {
		for key, val := range datum {
			if key == ".id" {
				continue
			}
			if _, ok := schemaCollection.Types[key]; !ok {
				return errors.New(fmt.Sprintf("Key does not exist in collection schema: %v", key))
			}

			switch schemaCollection.Types[key] {
			case "STRING":
				if _, ok := val.(string); !ok {
					return errors.New(fmt.Sprintf("Value '%v' at key '%v' in record %v does not conform to schema type %v", val, key, idx, schemaCollection.Types[key]))
				}
			case "INT":
				// To us it's an int value
				// But under the hood golang is converting it to a float64 when coming from a JSON
				// string
				// It won't be an issue since the data gets stored as text anyway
				if _, ok := val.(float64); !ok {
					if _, ok := val.(int); !ok {
						return errors.New(fmt.Sprintf("Value '%v' at key '%v' in record %v does not conform to schema type %v", val, key, idx, schemaCollection.Types[key]))
					}
				}
			case "FLOAT":
				if _, ok := val.(float64); !ok {
					return errors.New(fmt.Sprintf("Value '%v' at key '%v' in record %v does not conform to schema type %v", val, key, idx, schemaCollection.Types[key]))
				}
			case "BOOL":
				if _, ok := val.(bool); !ok {
					return errors.New(fmt.Sprintf("Value '%v' at key '%v' in record %v does not conform to schema type %v", val, key, idx, schemaCollection.Types[key]))
				}
			case "DICT":
				if _, ok := val.(map[string]interface{}); !ok {
					return errors.New(fmt.Sprintf("Value '%v' at key '%v' in record %v does not conform to schema type %v", val, key, idx, schemaCollection.Types[key]))
				}
			case "LIST":
				if _, ok := val.([]interface{}); !ok {
					return errors.New(fmt.Sprintf("Value '%v' at key '%v' in record %v does not conform to schema type %v", val, key, idx, schemaCollection.Types[key]))
				}
			}
		}
	}
	return nil
}
