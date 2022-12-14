package record

import (
	"bufio"
	"ceresdb/config"
	"ceresdb/cursor"
	"ceresdb/freespace"
	"ceresdb/index"
	"ceresdb/schema"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type ToWriteStruct struct {
	Data   []map[string]interface{}
	Blocks [][]int
}

type ToOverWriteStruct struct {
	Data    []map[string]interface{}
	Indices []int
}

func readData(dbIdent, colIdent, fileIdent string, blocks [][]int) ([]map[string]interface{}, error) {
	blockIdx := 0
	blockLen := len(blocks)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeRead)
	output := make([]map[string]interface{}, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		op, dat, _, err := cursor.Next(s, nil)
		if err != nil {
			return nil, err
		}
		switch op {
		case cursor.OpRead:
			output = append(output, dat)
		case cursor.OpNext:
			blockIdx += 1
			if blockIdx >= len(blocks) {
				break
			}
			cursor.Advance(blocks[blockIdx][0], blocks[blockIdx][1])
		}
		s, e = utils.ReadLine(r)
	}
	return output, nil
}

func writeData(dbIdent, colIdent, fileIdent string, blocks [][]int, data []map[string]interface{}, schemaData map[string]string) error {
	blockIdx := 0
	dataIdx := 0
	dataLen := len(data)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeWrite)
	newContents := make([]string, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		if dataIdx < dataLen {
			data[dataIdx][".id"] = fmt.Sprintf("%s.%d", fileIdent, cursor.Index+1)
			op, _, dat, err := cursor.Next(s, data[dataIdx])
			if err != nil {
				return err
			}
			switch op {
			case cursor.OpWrite:
				newContents = append(newContents, dat)
				index.Add(dbIdent, colIdent, data[dataIdx], schemaData)
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

func overwriteData(dbIdent, colIdent, fileIdent string, blocks [][]int, data []map[string]interface{}, schemaData map[string]string) error {
	blockIdx := 0
	dataIdx := 0
	dataLen := len(data)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeWrite)
	newContents := make([]string, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		if dataIdx < dataLen {
			op, datum, dat, err := cursor.Next(s, data[dataIdx])
			if err != nil {
				return err
			}
			switch op {
			case cursor.OpWrite:
				newContents = append(newContents, dat)
				index.Update(dbIdent, colIdent, datum, data[dataIdx], schemaData)
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

func patchData(dbIdent, colIdent, fileIdent string, blocks [][]int, data map[string]interface{}, schemaData map[string]string) error {
	blockIdx := 0
	blockLen := len(blocks)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModePatch)
	newContents := make([]string, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		op, datum, dat, err := cursor.Next(s, data)
		if err != nil {
			return err
		}
		switch op {
		case cursor.OpWrite:
			newContents = append(newContents, dat)
			newDatum := make(map[string]interface{})
			json.Unmarshal([]byte(dat), &newDatum)
			index.Update(dbIdent, colIdent, datum, newDatum, schemaData)
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

func deleteData(dbIdent, colIdent, fileIdent string, blocks [][]int, schemaData map[string]string) error {
	blockIdx := 0
	blockLen := len(blocks)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModeDelete)
	newContents := make([]string, 0)

	path := config.Config.DataDir + "/" + dbIdent + "/" + colIdent + "/" + fileIdent
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	s, e := utils.ReadLine(r)
	for e == nil {
		op, datum, dat, _ := cursor.Next(s, nil)
		switch op {
		case cursor.OpDelete:
			newContents = append(newContents, dat)
			index.Delete(dbIdent, colIdent, datum, schemaData)
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

func Delete(database, collection string, ids []string) error {
	schemaData := schema.Get(database, collection)

	toDelete := make(map[string][]int)

	// Determine which IDs from which files should be read
	for _, id := range ids {
		parts := strings.Split(id, ".")
		if val, ok := toDelete[parts[0]]; ok {
			idx, _ := strconv.Atoi(parts[1])
			toDelete[parts[0]] = append(val, idx)
		} else {
			idx, _ := strconv.Atoi(parts[1])
			toDelete[parts[0]] = []int{idx}
		}
	}

	db := freespace.FreeSpace.Databases[database]
	col := db.Collections[collection]
	if col.Files == nil {
		col.Files = make(map[string]freespace.FreeSpaceFile)
	}

	// Build up range blocks
	for key, val := range toDelete {
		blocks := utils.BuildRangeBlocks((val))
		err := deleteData(database, collection, key, blocks, schemaData)
		if err != nil {
			return err
		}

		// Record newly available free space
		fi := col.Files[key]
		fi.Full = false
		fi.Blocks = utils.CombineRangeBlocks(utils.BuildRangeBlocks(val), fi.Blocks)
		col.Files[key] = fi
	}

	if db.Collections == nil {
		db.Collections = make(map[string]freespace.FreeSpaceCollection)
	}
	db.Collections[collection] = col
	if freespace.FreeSpace.Databases == nil {
		freespace.FreeSpace.Databases = make(map[string]freespace.FreeSpaceDatabase)
	}
	freespace.FreeSpace.Databases[database] = db

	freespace.WriteFreeSpace()

	return nil
}

func Get(database, collection string, ids []string) ([]map[string]interface{}, error) {
	output := make([]map[string]interface{}, 0)
	toRead := make(map[string][]int)

	// Determine which IDs from which files should be read
	for _, id := range ids {
		parts := strings.Split(id, ".")
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
		data, err := readData(database, collection, key, blocks)
		if err != nil {
			return nil, err
		}
		output = append(output, data...)
	}

	return output, nil
}

func Post(database, collection string, data []map[string]interface{}) error {
	if err := schema.ValidateDataAgainstSchema(database, collection, data); err != nil {
		return err
	}
	schemaData := schema.Get(database, collection)

	recordsRemaining := len(data)
	toWrite := make(map[string]ToWriteStruct)

	db := freespace.FreeSpace.Databases[database]
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
		val := freespace.FreeSpaceFile{Full: false, Blocks: [][]int{{0, config.Config.StorageLineLimit - 1}}}
		path := config.Config.DataDir + "/" + database + "/" + collection + "/" + id
		// Write out the new contents
		output := ""
		for idx := 0; idx < config.Config.StorageLineLimit; idx++ {
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
		if col.Files == nil {
			col.Files = make(map[string]freespace.FreeSpaceFile)
		}
		col.Files[id] = val
	}

	if db.Collections == nil {
		db.Collections = make(map[string]freespace.FreeSpaceCollection)
	}
	db.Collections[collection] = col
	if freespace.FreeSpace.Databases == nil {
		freespace.FreeSpace.Databases = make(map[string]freespace.FreeSpaceDatabase)
	}
	freespace.FreeSpace.Databases[database] = db

	freespace.WriteFreeSpace()

	for key, val := range toWrite {
		err := writeData(database, collection, key, val.Blocks, val.Data, schemaData)
		if err != nil {
			return err
		}
	}

	return nil
}

func Patch(database, collection string, ids []string, data map[string]interface{}) error {
	if err := schema.ValidateDataAgainstSchema(database, collection, []map[string]interface{}{data}); err != nil {
		return err
	}
	schemaData := schema.Get(database, collection)

	toPatch := make(map[string][]int)

	// Determine which IDs from which files should be read
	for _, id := range ids {
		parts := strings.Split(id, ".")
		if val, ok := toPatch[parts[0]]; ok {
			idx, _ := strconv.Atoi(parts[1])
			toPatch[parts[0]] = append(val, idx)
		} else {
			idx, _ := strconv.Atoi(parts[1])
			toPatch[parts[0]] = []int{idx}
		}
	}

	// Build up range blocks
	for key, val := range toPatch {
		blocks := utils.BuildRangeBlocks((val))
		err := patchData(database, collection, key, blocks, data, schemaData)
		if err != nil {
			return err
		}
	}

	return nil
}

func Put(database, collection string, data []map[string]interface{}) error {
	if err := schema.ValidateDataAgainstSchema(database, collection, data); err != nil {
		return err
	}
	schemaData := schema.Get(database, collection)

	toOverWrite := make(map[string]ToOverWriteStruct)

	for _, datum := range data {
		parts := strings.Split(datum[".id"].(string), ".")
		if val, ok := toOverWrite[parts[0]]; ok {
			idx, _ := strconv.Atoi(parts[1])
			val.Indices = append(val.Indices, idx)
			val.Data = append(val.Data, datum)
			toOverWrite[parts[0]] = val
		} else {
			idx, _ := strconv.Atoi(parts[1])
			writable := ToOverWriteStruct{Indices: make([]int, 0), Data: make([]map[string]interface{}, 0)}
			writable.Indices = []int{idx}
			writable.Data = []map[string]interface{}{datum}
			toOverWrite[parts[0]] = writable
		}
	}

	// Build up range blocks
	for key, val := range toOverWrite {
		blocks := utils.BuildRangeBlocks((val.Indices))
		err := overwriteData(database, collection, key, blocks, val.Data, schemaData)
		if err != nil {
			return err
		}
	}

	return nil
}
