package schema

import (
	"ceresdb/config"
	"ceresdb/constants"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

var Schemas map[string]map[string]interface{}

func LoadSchemas() error {
	path := fmt.Sprintf("%s/schemas.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Schemas = make(map[string]map[string]interface{})
		return saveSchema()
	}

	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	json.Unmarshal(byteValue, &Schemas)
	return saveSchema()
}

// Validates if an object matches a schema for a specific database and collectios
// database and collection names are _not_ validated in this function, instead
// they should be validated in the query pre-processor
func ValidateSchema(database string, collection string, data interface{}) error {
	s := Schemas[database][collection]
	return validateDataAgainstSchema(".", s, data)
}

// Traverses the object to check comparing each object to the provided schema
func validateDataAgainstSchema(key string, schema, object interface{}) error {
	if _, ok := schema.(string); ok {
		typeName := schema.(string)
		switch typeName {
		case constants.DATATYPE_ANY:
			return nil
		case constants.DATATYPE_STRING:
			if _, ok := object.(string); !ok {
				return fmt.Errorf("value at %s does not match type %s", key, typeName)
			}
		case constants.DATATYPE_INT:
			if _, ok := object.(int); !ok {
				if _, ok := object.(float64); !ok {
					return fmt.Errorf("value at %s does not match type %s", key, typeName)
				}
			}
		case constants.DATATYPE_FLOAT:
			if _, ok := object.(float64); !ok {
				return fmt.Errorf("value at %s does not match type %s", key, typeName)
			}
		case constants.DATATYPE_BOOL:
			if _, ok := object.(bool); !ok {
				return fmt.Errorf("value at %s does not match type %s", key, typeName)
			}
		case constants.DATATYPE_BYTE:
			if _, ok := object.(byte); !ok {
				return fmt.Errorf("value at %s does not match type %s", key, typeName)
			}
		default:
			return fmt.Errorf("invalid type: %s", typeName)
		}
		return nil
	}

	if schemaMap, ok := schema.(map[string]interface{}); ok {
		if dict, ok := object.(map[string]interface{}); ok {
			for child, val := range dict {
				if child == constants.ID_KEY || child == constants.TIME_KEY {
					continue
				}
				if err := validateDataAgainstSchema(fmt.Sprintf("%s.%s", key, child), schemaMap[child], val); err != nil {
					return err
				}
			}
			return nil
		}
		return fmt.Errorf("value at %s does not match type dict", key)
	}

	if list, ok := object.([]interface{}); ok {
		for idx, val := range list {
			if err := validateDataAgainstSchema(fmt.Sprintf("%s[%d]", key, idx), schema.([]interface{})[0], val); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("value at %s does not match type list: %v | %v", key, schema, object)

}

// Validates a schema against the grammar
func BuildSchema(database, collection string, schema interface{}) error {
	if err := validateDataAgainstGrammar(".", schema); err != nil {
		return err
	}
	if Schemas == nil {
		Schemas = map[string]map[string]interface{}{}
	}
	if _, ok := Schemas[database]; !ok {
		Schemas[database] = map[string]interface{}{}
	}
	Schemas[database][collection] = schema
	return saveSchema()
}

// Traverses the object to check if each element is a valid schema
func validateDataAgainstGrammar(key string, schema interface{}) error {
	if typeName, ok := schema.(string); ok {
		switch typeName {
		case constants.DATATYPE_ANY:
		case constants.DATATYPE_STRING:
		case constants.DATATYPE_INT:
		case constants.DATATYPE_FLOAT:
		case constants.DATATYPE_BOOL:
		case constants.DATATYPE_BYTE:
		default:
			return fmt.Errorf("invalid type at %s: %s", key, typeName)
		}
		return nil
	}

	if dict, ok := schema.(map[string]interface{}); ok {
		for child, val := range dict {
			if err := validateDataAgainstGrammar(fmt.Sprintf("%s.%s", key, child), val); err != nil {
				return err
			}
		}
		return nil
	}
	if list, ok := schema.([]interface{}); ok {
		if len(list) != 1 {
			return fmt.Errorf("invalid list specification: %v, list must contain exatly one element representing its type", list)
		}
		return validateDataAgainstGrammar(fmt.Sprintf("%s[0]", key), list[0])
	}
	return fmt.Errorf("value at %s of type %T is invalid", key, schema)
}

func Delete(d, name string) error {
	delete(Schemas[d], name)
	return saveSchema()
}

func saveSchema() error {
	path := fmt.Sprintf("%s/schemas.json", config.Config.DataDir)
	data, err := json.Marshal(Schemas)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0777)
}
