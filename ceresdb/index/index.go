package index

import (
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/schema"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const STRING_TERM = "//x00"

//                db         col        var
var Indices map[string]map[string]interface{}
var IndexKeys map[string]map[string][]string
var IndexIDs map[string]map[string][]string
var IndexCache map[string]map[string][]interface{}

const STRING_SPLIT_CHAR = " "

func LoadIndices() error {
	path := fmt.Sprintf("%s/indices.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Indices = make(map[string]map[string]interface{})
		return nil
	}

	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	json.Unmarshal(byteValue, &Indices)
	return nil
}

func LoadIndexKeys() error {
	path := fmt.Sprintf("%s/index_keys.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		IndexKeys = make(map[string]map[string][]string)
		return nil
	}

	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	json.Unmarshal(byteValue, &IndexKeys)
	return nil
}

func LoadIndexIDs() error {
	path := fmt.Sprintf("%s/index_ids.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		IndexIDs = make(map[string]map[string][]string)
		return nil
	}

	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	json.Unmarshal(byteValue, &IndexIDs)
	return nil
}

func LoadIndexCache() error {
	path := fmt.Sprintf("%s/index_cache.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		IndexCache = make(map[string]map[string][]interface{})
		return nil
	}

	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	json.Unmarshal(byteValue, &IndexIDs)
	return nil
}

func Delete(d, c string) error {
	delete(Indices[d], c)
	delete(IndexKeys[d], c)
	delete(IndexIDs[d], c)
	delete(IndexCache[d], c)
	return saveIndices()
}

func BuildIndex(d, c string) error {
	s := schema.Schemas[d][c]
	if Indices == nil {
		Indices = map[string]map[string]interface{}{}
		IndexKeys = map[string]map[string][]string{}
		IndexIDs = map[string]map[string][]string{}
		IndexCache = map[string]map[string][]interface{}{}
	}
	if _, ok := Indices[d]; !ok {
		Indices[d] = map[string]interface{}{}
		IndexKeys[d] = map[string][]string{}
		IndexIDs[d] = map[string][]string{}
		IndexCache[d] = map[string][]interface{}{}
	}
	keys := []string{}
	var idx interface{}
	idx, keys = buildIndexAgainstSchema(s, keys, "")
	Indices[d][c] = idx
	IndexKeys[d][c] = keys
	IndexIDs[d][c] = []string{}
	IndexCache[d][c] = []interface{}{}
	return saveIndices()
}

// This functino assumes that the schema has already been verified
func buildIndexAgainstSchema(s interface{}, keys []string, key string) (interface{}, []string) {
	if typeName, ok := s.(string); ok {
		switch typeName {
		case constants.DATATYPE_STRING:
			return map[string][]string{}, keys
		case constants.DATATYPE_INT:
			return map[int][]string{}, keys
		case constants.DATATYPE_FLOAT:
			return map[float64][]string{}, keys
		case constants.DATATYPE_BOOL:
			return map[bool][]string{}, keys
		}
		return nil, keys
	}

	if dict, ok := s.(map[string]interface{}); ok {
		if len(dict) == 0 {
			return nil, keys
		}
		out := map[string]interface{}{}
		for child, val := range dict {
			var o interface{}
			o, keys = buildIndexAgainstSchema(val, keys, fmt.Sprintf("%s.%s", key, child))
			if o != nil {
				keys = append(keys, fmt.Sprintf("%s.%s", key, child))
			}
			out[child] = o
		}
		return out, keys
	}

	list := s.([]interface{})
	var o interface{}
	o, keys = buildIndexAgainstSchema(list[0], keys, key)
	if o != nil {
		keys = append(keys, key)
	}
	return o, keys
	// return nil, keys
}

func AddToIndex(d, c string, obj interface{}) error {
	s := schema.Schemas[d][c]
	objInterface := obj.(map[string]interface{})
	id := objInterface["_id"].(string)
	Indices[d][c] = addAgainstIndex(id, Indices[d][c], s, obj)
	IndexIDs[d][c] = append(IndexIDs[d][c], id)
	IndexCache[d][c] = append(IndexCache[d][c], obj)
	if len(IndexIDs) > config.Config.MaxIndexSize {
		DeleteFromIndex(d, c, IndexCache[d][c][0])
	}
	return saveIndices()
}

func addAgainstIndex(id string, idx, s, obj interface{}) interface{} {
	if idx == nil {
		return nil
	}
	if typeName, ok := s.(string); ok {
		switch typeName {
		case constants.DATATYPE_STRING:
			idxInterface := idx.(map[string][]string)
			key := obj.(string)
			temp := []string{}
			if val, ok := idxInterface[key]; ok {
				temp = val
			}
			temp = append(temp, id)
			idxInterface[key] = temp
			idx = idxInterface
		case constants.DATATYPE_INT:
			idxInterface := idx.(map[int][]string)
			key := 0
			if val, ok := obj.(int); ok {
				key = val
			} else {
				key = int(obj.(float64))
			}
			temp := []string{}
			if val, ok := idxInterface[key]; ok {
				temp = val
			}
			temp = append(temp, id)
			idxInterface[key] = temp
			idx = idxInterface
		case constants.DATATYPE_FLOAT:
			idxInterface := idx.(map[float64][]string)
			key := obj.(float64)
			temp := []string{}
			if val, ok := idxInterface[key]; ok {
				temp = val
			}
			temp = append(temp, id)
			idxInterface[key] = temp
			idx = idxInterface
		case constants.DATATYPE_BOOL:
			idxInterface := idx.(map[bool][]string)
			key := obj.(bool)
			temp := []string{}
			if val, ok := idxInterface[key]; ok {
				temp = val
			}
			temp = append(temp, id)
			idxInterface[key] = temp
			idx = idxInterface
		}
		return idx
	}
	if dict, ok := obj.(map[string]interface{}); ok {
		idxInterface := idx.(map[string]interface{})
		sInterface := s.(map[string]interface{})
		for key, child := range dict {
			if key == constants.ID_KEY || key == constants.TIME_KEY {
				continue
			}
			i := addAgainstIndex(id, idxInterface[key], sInterface[key], child)
			idxInterface[key] = i
		}
		return idxInterface
	}
	list := obj.([]interface{})
	for _, val := range list {
		sInterface := s.([]interface{})
		idx = addAgainstIndex(id, idx, sInterface[0], val)
	}
	return idx
}

func DeleteFromIndex(d, c string, obj interface{}) error {
	s := schema.Schemas[d][c]
	objInterface := obj.(map[string]interface{})
	id := objInterface["_id"].(string)
	Indices[d][c] = deleteAgainstIndex(id, Indices[d][c], s, obj)
	cacheIdx := utils.Index(IndexIDs[d][c], id)
	IndexIDs[d][c] = append(IndexIDs[d][c][:cacheIdx], IndexIDs[d][c][cacheIdx+1:]...)
	IndexCache[d][c] = append(IndexCache[d][c][:cacheIdx], IndexCache[d][c][cacheIdx+1:]...)
	return saveIndices()
}

func deleteAgainstIndex(id string, idx, s, obj interface{}) interface{} {
	if idx == nil {
		return nil
	}
	if typeName, ok := s.(string); ok {
		switch typeName {
		case constants.DATATYPE_STRING:
			idxInterface := idx.(map[string][]string)
			key := obj.(string)
			idxInterface[key] = utils.Remove(idxInterface[key], id)
			return idxInterface
		case constants.DATATYPE_INT:
			idxInterface := idx.(map[int][]string)
			key := 0
			if val, ok := obj.(int); ok {
				key = val
			} else {
				key = int(obj.(float64))
			}
			idxInterface[key] = utils.Remove(idxInterface[key], id)
			return idxInterface
		case constants.DATATYPE_FLOAT:
			idxInterface := idx.(map[float64][]string)
			key := obj.(float64)
			idxInterface[key] = utils.Remove(idxInterface[key], id)
			return idxInterface
		case constants.DATATYPE_BOOL:
			idxInterface := idx.(map[bool][]string)
			key := obj.(bool)
			idxInterface[key] = utils.Remove(idxInterface[key], id)
			return idxInterface
		}
		return nil
	}
	if dict, ok := obj.(map[string]interface{}); ok {
		idxInterface := idx.(map[string]interface{})
		sInterface := s.(map[string]interface{})
		for key, child := range dict {
			if key == constants.ID_KEY || key == constants.TIME_KEY {
				continue
			}
			idxInterface[key] = deleteAgainstIndex(id, idxInterface[key], sInterface[key], child)
		}
		return idxInterface
	}
	list := obj.([]interface{})
	for _, val := range list {
		sInterface := s.([]interface{})
		idx = deleteAgainstIndex(id, idx, sInterface[0], val)
	}
	return idx
}

func RetrieveFromIndex(d, c string, keys []string, val interface{}) ([]string, error) {
	s := schema.Schemas[d][c]
	idx := Indices[d][c]
	keyString := "." + strings.Join(keys, ".")
	if !utils.Contains(IndexKeys[d][c], keyString) {
		return nil, fmt.Errorf("key %s is not searchable in %s.%s: %v", keyString, d, c, IndexKeys[d][c])
	}
	out := retrieveAgainstIndex(keys, idx, s, val)
	return out, nil
}

func retrieveAgainstIndex(keys []string, idx, s, val interface{}) []string {
	if len(keys) == 0 {
		typeName := s.(string)
		switch typeName {
		case constants.DATATYPE_STRING:
			idxInterface := idx.(map[string][]string)
			value := val.(string)
			outInterface := idxInterface[value]
			out := outInterface
			return out
		case constants.DATATYPE_INT:
			idxInterface := idx.(map[int][]string)
			value := 0
			if v, ok := val.(int); ok {
				value = v
			} else {
				value = int(val.(float64))
			}

			outInterface := idxInterface[value]
			out := outInterface
			return out
		case constants.DATATYPE_FLOAT:
			idxInterface := idx.(map[float64][]string)
			value := val.(float64)
			outInterface := idxInterface[value]
			out := outInterface
			return out
		case constants.DATATYPE_BOOL:
			idxInterface := idx.(map[bool][]string)
			value := val.(bool)
			outInterface := idxInterface[value]
			out := outInterface
			return out
		}
		return []string{}
	}
	idxInterface := idx.(map[string]interface{})
	sInterface := s.(map[string]interface{})
	out := retrieveAgainstIndex(keys[1:], idxInterface[keys[0]], sInterface[keys[0]], val)
	return out
}

func RetrieveValsFromIndex(d, c string, keys []string) (string, interface{}, error) {
	s := schema.Schemas[d][c]
	idx := Indices[d][c]
	keyString := "." + strings.Join(keys, ".")
	if !utils.Contains(IndexKeys[d][c], keyString) {
		return "", nil, fmt.Errorf("key %s is not searchable in %s.%s: %v", keyString, d, c, IndexKeys[d][c])
	}
	dType, out := retrieveValsAgainstIndex(keys, idx, s)
	return dType, out, nil
}

func retrieveValsAgainstIndex(keys []string, idx, s interface{}) (string, interface{}) {
	if len(keys) == 0 {
		typeName := s.(string)
		switch typeName {
		case constants.DATATYPE_STRING:
			out := idx.(map[string][]string)
			return typeName, out
		case constants.DATATYPE_INT:
			out := idx.(map[int][]string)
			return typeName, out
		case constants.DATATYPE_FLOAT:
			out := idx.(map[float64][]string)
			return typeName, out
		case constants.DATATYPE_BOOL:
			out := idx.(map[bool][]string)
			return typeName, out
		}
		return fmt.Sprintf("type %s is not valid", typeName), nil
		// return typeName, nil
	}
	idxInterface := idx.(map[string]interface{})
	sInterface := s.(map[string]interface{})
	dType, out := retrieveValsAgainstIndex(keys[1:], idxInterface[keys[0]], sInterface[keys[0]])
	return dType, out
}

func saveIndices() error {
	path := fmt.Sprintf("%s/indices.json", config.Config.DataDir)
	data, err := json.Marshal(Indices)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0777); err != nil {
		return err
	}
	path2 := fmt.Sprintf("%s/index_keys.json", config.Config.DataDir)
	data2, err := json.Marshal(IndexKeys)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path2, data2, 0777); err != nil {
		return err
	}
	path3 := fmt.Sprintf("%s/indices.json", config.Config.DataDir)
	data3, err := json.Marshal(IndexKeys)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path3, data3, 0777); err != nil {
		return err
	}
	path4 := fmt.Sprintf("%s/index_cache.json", config.Config.DataDir)
	data4, err := json.Marshal(IndexKeys)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path4, data4, 0777); err != nil {
		return err
	}
	path5 := fmt.Sprintf("%s/index_insert.json", config.Config.DataDir)
	data5, err := json.Marshal(IndexKeys)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path5, data5, 0777); err != nil {
		return err
	}
	return nil
}
