// manager.go

package manager

import (
	"bufio"
	"ceres/config"
	"ceres/cursor"
	log "ceres/logging"
	"ceres/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
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

type ToWriteStruct struct {
	Data   []map[string]interface{}
	Blocks [][]int
}

var FreeSpace FreeSpaceStruct

func LoadFreeSpace() error {
	log.DEBUG("Loading free space file...")

	path := config.Config.CeresDir + "/free_space.json"

	// Open our jsonFile
	jsonFile, err := os.Open(path)

	// If we os.Open returns an error then handle it
	if err != nil {
		log.ERROR("Unable to read json file")
		return err
	}

	log.INFO("Successfully Opened " + path)

	// Read and unmarshal the JSON
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var f interface{}

	// Read the JSON
	err = json.Unmarshal(byteValue, &f)
	if err != nil {
		log.ERROR("Unable to unmarshal json file")
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

func read(dbIdent, colIdent, fileIdent string, blocks [][]int) ([]map[string]interface{}, error) {
	blockIdx := 0
	blockLen := len(blocks)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeRead)
	output := make([]map[string]interface{}, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		log.ERROR("Could not open file: " + path)
		return nil, err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		op, dat, _, err := cursor.Next(s, nil)
		if err != nil {
			log.ERROR("Cursor error")
			return nil, err
		}
		switch op {
		case cursor.OpRead:
			output = append(output, dat)
		case cursor.OpNext:
			blockIdx += 1
			if blockIdx == blockLen {
				break
			}
			cursor.Advance(blocks[blockIdx][0], blocks[blockIdx][1])
		}
		s, e = utils.ReadLine(r)
	}
	return output, nil
}

func write(dbIdent, colIdent, fileIdent string, blocks [][]int, data []map[string]interface{}) error {
	blockIdx := 0
	dataIdx := 0
	dataLen := len(data)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeWrite)
	newContents := make([]string, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		log.ERROR("Could not open file: " + path)
		return err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		if dataIdx < dataLen {
			data[dataIdx][".id"] = fmt.Sprintf("%s-%d", fileIdent, cursor.Index+1)
			op, _, dat, err := cursor.Next(s, data[dataIdx])
			if err != nil {
				log.ERROR("Cursor error")
				return err
			}
			switch op {
			case cursor.OpWrite:
				newContents = append(newContents, dat)
				dataIdx += 1
			case cursor.OpJump:
				newContents = append(newContents, s+"\n")
			case cursor.OpNext:
				blockIdx += 1
				newContents = append(newContents, s+"\n")
				cursor.Advance(blocks[blockIdx][0], blocks[blockIdx][1])
			}
		} else {
			newContents = append(newContents, s+"\n")
		}
		s, e = utils.ReadLine(r)
	}

	// Write out the new contents
	output := strings.Join(newContents[:], "")
	f, _ = os.Create(path)
	defer f.Close()
	f.Write([]byte(output))

	return nil
}

func delete(dbIdent, colIdent, fileIdent string, blocks [][]int) error {
	blockIdx := 0
	blockLen := len(blocks)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeDelete)
	newContents := make([]string, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		log.ERROR("Could not open file: " + path)
		return err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		op, _, dat, _ := cursor.Next(s, nil)
		switch op {
		case cursor.OpDelete:
			newContents = append(newContents, dat)
		case cursor.OpJump:
			newContents = append(newContents, s+"\n")
		case cursor.OpNext:
			blockIdx += 1
			newContents = append(newContents, s+"\n")
			if blockIdx == blockLen {
				break
			}
			cursor.Advance(blocks[blockIdx][0], blocks[blockIdx][1])
		}
		s, e = utils.ReadLine(r)
	}

	// Write out the new contents
	output := strings.Join(newContents[:], "")
	f, _ = os.Create(path)
	defer f.Close()
	f.Write([]byte(output))

	return nil
}

func Read(database, collection string, ids []string) ([]map[string]interface{}, error) {
	output := make([]map[string]interface{}, 0)
	toRead := make(map[string][]int)

	// Determine which IDs from which files should be read
	for _, id := range ids {
		parts := strings.Split(id, "-")
		if val, ok := toRead[parts[0]]; ok {
			idx, _ := strconv.Atoi(parts[1])
			toRead[parts[0]] = append(val, idx)
		} else {
			idx, _ := strconv.Atoi(parts[1])
			toRead[parts[0]] = []int{idx}
		}
	}

	// Build up range blocks
	for key, val := range toRead {
		blocks := utils.BuildRangeBlocks(val)
		data, err := read(database, collection, key, blocks)
		if err != nil {
			return nil, err
		}
		output = append(output, data...)
	}

	return output, nil
}

func Write(database, collection string, data []map[string]interface{}) error {
	recordsRemaining := len(data)
	toWrite := make(map[string]ToWriteStruct)

	db := FreeSpace.Databases[database]
	col := db.Collections[collection]

	keys := make([]string, 0, len(col.Files))
	for k := range col.Files {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := col.Files[key]
		// If the file is not full
		if !val.Full {
			// Direct data to be written to the file
			writable := ToWriteStruct{Blocks: make([][]int, 0), Data: make([]map[string]interface{}, 0)}
			for _, block := range val.Blocks {
				log.INFO(fmt.Sprintf("Block size: %d", recordsRemaining))
				blockSize := block[1] - block[0] + 1
				if recordsRemaining >= blockSize {
					// Handle more records than is available in the block
					writable.Blocks = append(writable.Blocks, []int{block[0], block[1]})
					writable.Data = append(writable.Data, data[:blockSize]...)
					data = data[blockSize:]
					recordsRemaining -= blockSize
					val.Blocks = val.Blocks[1:]
					continue
				} else {
					// Handle a larger block than there are records available
					writable.Blocks = append(writable.Blocks, []int{block[0], block[0] + recordsRemaining - 1})
					writable.Data = append(writable.Data, data...)
					block[0] = block[0] + recordsRemaining
					recordsRemaining = 0
					val.Blocks[0] = block
					break
				}
			}
			toWrite[key] = writable
			if len(val.Blocks) == 0 {
				val.Full = true
			}
			col.Files[key] = val
		}
		if recordsRemaining == 0 {
			break
		}
	}

	// Handle overflow
	for recordsRemaining > 0 {
		id := uuid.New().String()
		val := FreeSpaceFile{Full: false, Blocks: [][]int{{0, config.Config.StorageLineLimit - 1}}}
		path := config.Config.DataDir + "/" + database + "/" + collection + "/" + id
		// Write out the new contents
		output := ""
		for idx := 0; idx < 32; idx++ {
			output += "\n"
		}
		f, _ := os.Create(path)
		f.Write([]byte(output))
		f.Close()
		writable := ToWriteStruct{Blocks: make([][]int, 0), Data: make([]map[string]interface{}, 0)}
		for _, block := range val.Blocks {
			blockSize := block[1] - block[0] + 1
			if recordsRemaining >= blockSize {
				// Handle more records than is available in the block
				writable.Blocks = append(writable.Blocks, []int{block[0], block[1]})
				writable.Data = append(writable.Data, data[:blockSize]...)
				data = data[blockSize:]
				recordsRemaining -= blockSize
				val.Blocks = val.Blocks[1:]
				continue
			} else {
				// Handle a larger block than there are records available
				writable.Blocks = append(writable.Blocks, []int{block[0], block[0] + recordsRemaining - 1})
				writable.Data = append(writable.Data, data...)
				block[0] = block[0] + recordsRemaining
				recordsRemaining = 0
				val.Blocks[0] = block
				break
			}
		}
		toWrite[id] = writable
		if len(val.Blocks) == 0 {
			val.Full = true
		}
		col.Files[id] = val
	}

	db.Collections[collection] = col
	FreeSpace.Databases[database] = db

	for key, val := range toWrite {
		err := write(database, collection, key, val.Blocks, val.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

func Delete(database, collection string, ids []string) error {
	toDelete := make(map[string][]int)

	// Determine which IDs from which files should be read
	for _, id := range ids {
		parts := strings.Split(id, "-")
		if val, ok := toDelete[parts[0]]; ok {
			idx, _ := strconv.Atoi(parts[1])
			toDelete[parts[0]] = append(val, idx)
		} else {
			idx, _ := strconv.Atoi(parts[1])
			toDelete[parts[0]] = []int{idx}
		}
	}

	db := FreeSpace.Databases[database]
	col := db.Collections[collection]

	// Build up range blocks
	for key, val := range toDelete {
		blocks := utils.BuildRangeBlocks((val))
		err := delete(database, collection, key, blocks)
		if err != nil {
			return err
		}

		// Record newly available free space
		fi := col.Files[key]
		fi.Full = false
		fi.Blocks = utils.CombineRangeBlocks(utils.BuildRangeBlocks(val), fi.Blocks)
		col.Files[key] = fi
	}

	db.Collections[collection] = col
	FreeSpace.Databases[database] = db

	return nil
}
