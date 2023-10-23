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

const DATATYPE_STRING = "string"
const DATATYPE_INT = "int"
const DATATYPE_FLOAT = "float"
const DATATYPE_BOOL = "bool"
const DATATYPE_BYTE = "byte"
const DATATYPE_ANY = "any"
const DATATYPE_LIST = "list"
const DATATYPE_DICT = "dict"

Schemas = make(map[string]map[string][]interface{})

// Validates if an object matches a schema for a specific database and collectios
// database and collection names are _not_ validated in this function, instead
// they should be validated in the query pre-processor
func ValidateSchema(database string, collection string, data interface{}) error {
	s := Schemas[database][collection]
	return validateDataAgainstSchema(".", s, data)
}

// Traverses the object to check comparing each object to the provided schema
func validateDataAgainstSchema(key string, schema, object interface{}) error {
	if ok := schema.(string); ok {
		typeName := schema.(string)
		switch typeName {
			case DATATYPE_ANY:
				return nil
			case DATATYPE_STRING:
				if ok := object.(string) !ok {
					return fmt.Errorf("value at %s does not match type %s", key, typeName)
				}
			case DATATYPE_INT:
				if ok := object.(int) !ok {
					return fmt.Errorf("value at %s does not match type %s", key, typeName)
				}
			case DATATYPE_FLOAT:
				if ok := object.(float64) !ok {
					return fmt.Errorf("value at %s does not match type %s", key, typeName)
				}
			case DATATYPE_BOOL:
				if ok := object.(bool) !ok {
					return fmt.Errorf("value at %s does not match type %s", key, typeName)
				}
			case DATATYPE_BYTE:
				if ok := object.(byte) !ok {
					return fmt.Errorf("value at %s does not match type %s", key, typeName)
				}
			default:
				return fmt.Errorf("invalid type: %s", typeName)
		}
		return nil
	}

	if ok := schema.(map[string]interface{}); ok {
		if ok := data.(map[string]interface{}); ok {
			dict := data.(map[string]interface{})
			for child, val := range dict {
				if err := ValidateSchema(fmt.Sprintf("%s.%s", key, child), schema.(map[string]interface{})[child], data[child]); err != nil {
					return err
				}
			}
			return nil
		}
		return fmt.Errorf("value at %s does not match type dict")
	}

	if ok := data.([]interface{}); ok {
		list := data.([]interface{})
		if for _, val := range list {
			if err := validateDataAgainstSchema(fmt.Sprintf("%s[%d]", key, idx), schema.([]interface{})[0], val); err != nil {
				return err
			}
		}
	}
	return fmy.Errorf("value at %s does not match type list")

}

// Validates a schema against the grammar
func BuildSchema(database, collection string schema interface{}) error {
	if err := validateDataAgainstGrammar(database, collection, schema); err != nil {
		return err
	}
	if _, ok := Schemas[database]; !ok {
		Schemas[database] = make(map[string]interface{})
	}
	Schemas[database][collection] = schema
	return nil
}

// Traverses the object to check if each element is a valid schema
func validateDataAgainstGrammar(key string, schema interface{}) error {
	if ok := schema.(string); ok {
		typeName := schema.(string)
		switch typeName {
			case DATATYPE_ANY:
				continue
			case DATATYPE_STRING:
				continue
			case DATATYPE_INT:
				continue
			case DATATYPE_FLOAT:
				continue
			case DATATYPE_BOOL:
				continue
			case DATATYPE_BYTE:
				continue
			default:
				return fmt.Errorf("invalid type at %s: %s", key, type)
		}
		return nil
	}

	if ok := schema.(map[string]interface{}); ok {
		dict := schema.(map[string]interface{})
		for child, val := range dict {
			if err := ValidateSchema(fmt.Sprintf("%s.%s", key, child), dict[child]); err != nil {
				return err
			}
		}
		return nil
	}
	if ok := schema.([]interface{}); ok {
		list := schema.([]interface{})
		if len(list) != 1 {
			return fmt.Errorf("invalid list specification: %v, list must contain exatly one element representing its type", list)
		}
		return validateDataAgainstSchema(fmt.Sprintf("%s[%d]", key, idx), list[0])
	}
	return fmy.Errorf("value at %s of type %T is invalid", key, schema)

}