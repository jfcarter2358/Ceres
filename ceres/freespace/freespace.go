// schema.go

package freespace

import (
	"ceres/config"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

type FreeSpaceFile struct {
	Full   bool    `json:"full"`
	Blocks [][]int `json:"blocks"`
}

type FreeSpaceCollection struct {
	Files map[string]FreeSpaceFile
}

type FreeSpaceDatabase struct {
	Collections map[string]FreeSpaceCollection
}

type FreeSpaceStruct struct {
	Databases map[string]FreeSpaceDatabase
}

var FreeSpace FreeSpaceStruct

func LoadFreeSpace() error {
	path := filepath.Join(config.Config.HomeDir, "free_space.json")

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
	FreeSpace.Databases = make(map[string]FreeSpaceDatabase)

	// Loop through the top-level items (database names)
	for dbKey, dbVal := range itemsMap {
		dbItem := FreeSpaceDatabase{}
		dbItem.Collections = make(map[string]FreeSpaceCollection)
		dbItemsMap := dbVal.(map[string]interface{})

		// Loop through the 2nd-level items (collection names)
		for colKey, colVal := range dbItemsMap {
			colItem := FreeSpaceCollection{}
			colItem.Files = make(map[string]FreeSpaceFile)
			colItemsMap := colVal.(map[string]interface{})

			// Loop through the 3rd-level items (file names)
			for fileKey, fileVal := range colItemsMap {
				fileItem := FreeSpaceFile{}
				fileItemsMap := fileVal.(map[string]interface{})
				fileItem.Full = fileItemsMap["full"].(bool)
				blocksItem := make([][]int, 0)
				blocksItemList := fileItemsMap["blocks"].([]interface{})

				// Loop through the block items
				for _, blockVal := range blocksItemList {
					blockItem := make([]int, 0)
					blockItemList := blockVal.([]interface{})
					for _, idxVal := range blockItemList {
						blockItem = append(blockItem, int(idxVal.(float64)))
					}
					blocksItem = append(blocksItem, blockItem)
				}

				fileItem.Blocks = blocksItem
				colItem.Files[fileKey] = fileItem
			}
			dbItem.Collections[colKey] = colItem
		}
		FreeSpace.Databases[dbKey] = dbItem
	}

	return nil
}

func WriteFreeSpace() error {
	path := filepath.Join(config.Config.HomeDir, "free_space.json")

	output := make(map[string]interface{})
	for dbKey, db := range FreeSpace.Databases {
		dbInterface := make(map[string]interface{})
		for colKey, col := range db.Collections {
			colInterface := make(map[string]interface{})
			for fileKey, file := range col.Files {
				fileInterface := map[string]interface{}{"full": file.Full, "blocks": file.Blocks}
				colInterface[fileKey] = fileInterface
			}
			dbInterface[colKey] = colInterface
		}
		output[dbKey] = dbInterface
	}

	freeSpaceContents, _ := json.MarshalIndent(output, "", "    ")
	_ = ioutil.WriteFile(path, freeSpaceContents, 0644)

	return nil
}
