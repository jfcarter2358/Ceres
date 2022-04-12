// index.go

package index

import (
	"ceres/config"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func Add(database, collection string, datum map[string]interface{}) error {
	for key, val := range datum {
		if key == ".id" {
			continue
		}
		if key == "password" && database == "_auth" {
			continue
		}
		stringVal := fmt.Sprintf("%v", val)
		dirPath := filepath.Join(config.Config.IndexDir, database, collection, key)
		filePath := filepath.Join(dirPath, stringVal)
		allPath := filepath.Join(config.Config.IndexDir, database, collection, "all")
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			err = os.Mkdir(dirPath, 0755)
			if err != nil {
				return err
			}
		}
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		f.WriteString(datum[".id"].(string) + "\n")
		f.Close()
		f, err = os.OpenFile(allPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		f.WriteString(datum[".id"].(string) + "\n")
		f.Close()
	}
	return nil
}

func Delete(database, collection string, datum map[string]interface{}) error {
	for key, val := range datum {
		if key == ".id" {
			continue
		}
		if key == "password" && database == "_auth" {
			continue
		}
		stringVal := fmt.Sprintf("%v", val)
		filePath := filepath.Join(config.Config.IndexDir, database, collection, key, stringVal)
		data, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		indices := strings.Split(string(data), "\n")
		indices = removeIndex(indices, datum[".id"].(string))
		if len(indices) == 1 {
			if err = os.Remove(filePath); err != nil {
				return err
			}
		} else {
			os.WriteFile(filePath, []byte(strings.Join(indices, "\n")), 0644)
		}
		allPath := filepath.Join(config.Config.IndexDir, database, collection, "all")
		data, err = os.ReadFile(allPath)
		if err != nil {
			return err
		}
		indices = strings.Split(string(data), "\n")
		indices = removeIndex(indices, datum[".id"].(string))
		os.WriteFile(allPath, []byte(strings.Join(indices, "\n")), 0644)
	}
	return nil
}

func Update(database, collection string, oldDatum, newDatum map[string]interface{}) error {
	if err := Delete(database, collection, oldDatum); err != nil {
		return err
	}
	if err := Add(database, collection, newDatum); err != nil {
		return err
	}
	return nil
}

func Get(database, collection, key, value string) ([]string, error) {
	filePath := filepath.Join(config.Config.IndexDir, database, collection, key, value)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	indices := strings.Split(string(data), "\n")
	return indices[:len(indices)-1], nil
}

func All(database, collection string) ([]string, error) {
	filePath := filepath.Join(config.Config.IndexDir, database, collection, "all")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	indices := strings.Split(string(data), "\n")
	return indices[:len(indices)-1], nil
}

func removeIndex(indices []string, index string) []string {
	idx := linearSearch(indices, index)
	if idx != -1 {
		return append(indices[:idx], indices[idx+1:]...)
	}
	return indices
}

func linearSearch(s []string, val string) int {
	for i, v := range s {
		if v == val {
			return i
		}
	}
	return -1
}
