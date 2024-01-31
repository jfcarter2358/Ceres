package record

import (
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/index"
	"ceresdb/logger"
	"ceresdb/schema"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func Delete(d, c, id string) error {
	err := doDelete(d, c, id)
	return err
}

func Get(d, c, id string) (interface{}, error) {
	out, err := doRead(d, c, id)
	return out, err
}

func GetAllIndex(d, c string) ([]interface{}, error) {
	output := []interface{}{}
	logger.Debugf("", "Searching through index IDs")
	for _, id := range index.IndexIDs[d][c] {
		logger.Tracef("", "got id %s id", id)
		r, err := Get(d, c, id)
		if err != nil {
			return nil, err
		}
		output = append(output, r)
	}
	return output, nil
}

func GetAllColdStorage(d, c string) ([]interface{}, error) {
	output := []interface{}{}
	for f, fi := range collection.Collections[d][c].Files {
		if fi.Length > 0 {
			path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, f)
			lines, err := ReadDataFile(path)
			if err != nil {
				return nil, err
			}
			for _, line := range lines {
				if len(line) > 0 {
					var val interface{}
					if err := json.Unmarshal([]byte(line), &val); err != nil {
						return nil, err
					}
					output = append(output, val)
				}
			}
		}
	}
	return output, nil
}

func Update(d, c, id string, data interface{}) error {
	if err := schema.ValidateSchema(d, c, data); err != nil {
		return err
	}
	temp := data.(map[string]interface{})
	temp[constants.ID_KEY] = id
	return doUpdate(d, c, id, temp)
}

func Write(d, c string, data interface{}) error {
	if err := schema.ValidateSchema(d, c, data); err != nil {
		return err
	}
	temp := data.(map[string]interface{})
	err := doWrite(d, c, temp)
	return err
}

func doDelete(d, c, id string) error {
	parts := strings.Split(id, ".")
	if len(parts) != 2 {
		return fmt.Errorf("Invalid ID, must be of format <uuid>.<int>")
	}
	path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, parts[0])
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("Invalid ID, must be of format <uuid>.<int>")
	}
	lines, err := ReadDataFile(path)
	if err != nil {
		return err
	}
	if idx > len(lines) {
		return fmt.Errorf("Record with ID %s does not exist", id)
	}
	if lines[idx] == "" {
		return fmt.Errorf("Record with ID %s does not exist", id)
	}
	temp := lines[idx]
	lines[idx] = ""

	fi := collection.Collections[d][c].Files[parts[0]]
	col := collection.Collections[d][c]
	col.Files[parts[0]] = fi
	fi.Length -= 1
	fi.Available[idx] = 0
	collection.Collections[d][c] = col
	cPath := fmt.Sprintf("%s/collections.json", config.Config.DataDir)
	data, err := json.Marshal(collection.Collections)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cPath, data, 0777); err != nil {
		return err
	}

	var val interface{}
	if err := json.Unmarshal([]byte(temp), &val); err != nil {
		return err
	}
	index.DeleteFromIndex(d, c, val)

	if fi.Length == 0 {
		return os.Remove(path)
	}

	return writeDataFile(path, lines)
}

func doRead(d, c, id string) (interface{}, error) {
	parts := strings.Split(id, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Invalid ID, must be of format <uuid>.<int>")
	}
	path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, parts[0])
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("Invalid ID, must be of format <uuid>.<int>")
	}
	lines, err := ReadDataFile(path)
	if err != nil {
		return nil, err
	}
	if idx > len(lines) {
		return nil, fmt.Errorf("Record with ID %s does not exist", id)
	}
	if lines[idx] == "" {
		return nil, fmt.Errorf("Record with ID %s does not exist", id)
	}

	var val interface{}
	if err := json.Unmarshal([]byte(lines[idx]), &val); err != nil {
		return nil, err
	}
	return val, nil
}

func doUpdate(d, c, id string, val interface{}) error {
	parts := strings.Split(id, ".")
	if len(parts) != 2 {
		return fmt.Errorf("Invalid ID, must be of format <uuid>.<int>")
	}
	path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, parts[0])
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("Invalid ID, must be of format <uuid>.<int>")
	}
	lines, err := ReadDataFile(path)
	if err != nil {
		return err
	}
	if idx > len(lines) {
		return fmt.Errorf("Record with ID %s does not exist", id)
	}
	if lines[idx] == "" {
		return fmt.Errorf("Record with ID %s does not exist", id)
	}

	new := val.(map[string]interface{})
	var current map[string]interface{}
	if err := json.Unmarshal([]byte(lines[idx]), &current); err != nil {
		return err
	}
	now := time.Now()
	current[constants.TIME_KEY] = now.Unix()
	for key, val := range new {
		current[key] = val
	}
	data, err := json.Marshal(current)
	if err != nil {
		return err
	}
	lines[idx] = string(data)

	index.DeleteFromIndex(d, c, val)
	index.AddToIndex(d, c, current)

	return writeDataFile(path, lines)
}

func doWrite(d, c string, datum map[string]interface{}) error {

	for f, fi := range collection.Collections[d][c].Files {
		if fi.Length < config.Config.StorageLineLimit {
			for idx, val := range fi.Available {
				if val == 0 {
					id := fmt.Sprintf("%s.%d", f, idx)
					fi.Available[idx] = 1
					fi.Length += 1

					datum[constants.ID_KEY] = id
					now := time.Now()
					datum[constants.TIME_KEY] = now.Unix()
					path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, f)

					lines, err := ReadDataFile(path)
					if err != nil {
						return err
					}

					dataBytes, err := json.Marshal(datum)
					if err != nil {
						return err
					}
					if idx < len(lines) {
						lines[idx] = string(dataBytes)
					} else {
						lines = append(lines, string(dataBytes))
					}
					col := collection.Collections[d][c]
					col.Files[f] = fi
					collection.Collections[d][c] = col
					cPath := fmt.Sprintf("%s/collections.json", config.Config.DataDir)
					data, err := json.Marshal(collection.Collections)
					if err != nil {
						return err
					}
					if err := os.WriteFile(cPath, data, 0777); err != nil {
						return err
					}

					index.AddToIndex(d, c, datum)
					return writeDataFile(path, lines)
				}
			}
		}
	}

	f := uuid.New().String()
	fi := collection.File{
		Length:    1,
		Available: make([]int, config.Config.StorageLineLimit),
	}

	id := fmt.Sprintf("%s.0", f)
	fi.Available[0] = 1

	datum[constants.ID_KEY] = id
	now := time.Now()
	datum[constants.TIME_KEY] = now.Unix()
	path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, f)

	lines, err := ReadDataFile(path)
	if err != nil {
		return err
	}

	dataBytes, err := json.Marshal(datum)
	if err != nil {
		return err
	}
	lines = append(lines, string(dataBytes))

	col := collection.Collections[d][c]
	col.Files[f] = fi
	collection.Collections[d][c] = col
	cPath := fmt.Sprintf("%s/collections.json", config.Config.DataDir)
	data, err := json.Marshal(collection.Collections)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cPath, data, 0777); err != nil {
		return err
	}

	index.AddToIndex(d, c, datum)
	return writeDataFile(path, lines)
}

func ReadDataFile(path string) ([]string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		parent := strings.Join(strings.Split(path, "/")[:len(strings.Split(path, "/"))-1], "/")
		os.MkdirAll(parent, 0777)
		return []string{}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	return lines, nil
}

func writeDataFile(path string, lines []string) error {
	data := strings.Join(lines, "\n")
	err := os.WriteFile(path, []byte(data), 0777)
	return err
}
