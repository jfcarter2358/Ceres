// manager.go

package manager

import (
	"bufio"
	"ceres/aql"
	"ceres/config"
	"ceres/cursor"
	"ceres/freespace"
	"ceres/index"
	log "ceres/logging"
	"ceres/schema"
	"ceres/utils"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
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

func writeData(dbIdent, colIdent, fileIdent string, blocks [][]int, data []map[string]interface{}) error {
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
			data[dataIdx][".id"] = fmt.Sprintf("%s.%d", fileIdent, cursor.Index+1)
			op, _, dat, err := cursor.Next(s, data[dataIdx])
			if err != nil {
				log.ERROR("Cursor error")
				return err
			}
			switch op {
			case cursor.OpWrite:
				newContents = append(newContents, dat)
				index.Add(dbIdent, colIdent, data[dataIdx])
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

func overwriteData(dbIdent, colIdent, fileIdent string, blocks [][]int, data []map[string]interface{}) error {
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
			op, datum, dat, err := cursor.Next(s, data[dataIdx])
			if err != nil {
				log.ERROR("Cursor error")
				return err
			}
			switch op {
			case cursor.OpWrite:
				newContents = append(newContents, dat)
				index.Update(dbIdent, colIdent, datum, data[dataIdx])
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

func patchData(dbIdent, colIdent, fileIdent string, blocks [][]int, data map[string]interface{}) error {
	blockIdx := 0
	blockLen := len(blocks)
	cursor.Initialize(blocks[0][0], blocks[0][1], cursor.ModePatch)
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
		op, datum, dat, err := cursor.Next(s, data)
		if err != nil {
			log.ERROR("Cursor error")
			return err
		}
		switch op {
		case cursor.OpWrite:
			newContents = append(newContents, dat)
			newDatum := make(map[string]interface{})
			json.Unmarshal([]byte(dat), &newDatum)
			index.Update(dbIdent, colIdent, datum, newDatum)
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

func deleteData(dbIdent, colIdent, fileIdent string, blocks [][]int) error {
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
		op, datum, dat, _ := cursor.Next(s, nil)
		switch op {
		case cursor.OpDelete:
			newContents = append(newContents, dat)
			index.Delete(dbIdent, colIdent, datum)
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

func Write(database, collection string, data []map[string]interface{}) error {
	if err := schema.ValidateDataAgainstSchema(database, collection, data); err != nil {
		return err
	}

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
		err := writeData(database, collection, key, val.Blocks, val.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

func OverWrite(database, collection string, data []map[string]interface{}) error {
	if err := schema.ValidateDataAgainstSchema(database, collection, data); err != nil {
		return err
	}

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
		err := overwriteData(database, collection, key, blocks, val.Data)
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
		err := patchData(database, collection, key, blocks, data)
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
		err := deleteData(database, collection, key, blocks)
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

func ProcessAction(action aql.Action, previousIDs []string) ([]map[string]interface{}, error) {
	switch action.Type {
	case "GET":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		var ids []string
		var err error
		if action.Filter.Value != "" {
			ids, err = ProcessFilter(database, collection, action.Filter)
		} else {
			ids, err = index.All(database, collection)
		}
		if err != nil {
			return nil, err
		}
		data, err := Read(database, collection, ids)
		if err != nil {
			return nil, err
		}
		if action.OrderDir == "ASC" {
			data = doOrderASC(data, action.Order)
		} else if action.OrderDir == "DSC" {
			data = doOrderDSC(data, action.Order)
		}
		if action.Limit > 0 && action.Limit < len(data) {
			data = data[:action.Limit]
		}
		if len(data) > 0 && len(action.Fields) > 0 {
			if action.Fields[0] != "*" {
				keys := make([]string, len(data[0])-len(action.Fields))
				i := 0
				for k := range data[0] {
					if !utils.Contains(action.Fields, k) {
						keys[i] = k
						i++
					}
				}
				for idx, val := range data {
					for _, key := range keys {
						delete(val, key)
					}
					data[idx] = val
				}
			}
		}

		return data, nil
	case "COUNT":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		var ids []string
		var err error
		if action.Filter.Value != "" {
			ids, err = ProcessFilter(database, collection, action.Filter)
		} else {
			ids, err = index.All(database, collection)
		}
		if err != nil {
			return nil, err
		}
		data, err := Read(database, collection, ids)
		if err != nil {
			return nil, err
		}
		if action.OrderDir == "ASC" {
			data = doOrderASC(data, action.Order)
		} else if action.OrderDir == "DSC" {
			data = doOrderDSC(data, action.Order)
		}
		if action.Limit > 0 && action.Limit < len(data) {
			data = data[:action.Limit]
		}

		output := []map[string]interface{}{{"count": len(data)}}

		return output, nil
	case "POST":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		err := Write(database, collection, action.Data)
		if err != nil {
			return nil, err
		}
	case "PUT":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		err := OverWrite(database, collection, action.Data)
		if err != nil {
			return nil, err
		}
	case "PATCH":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		if action.IDs[0] != "-" {
			err := Patch(database, collection, action.IDs, action.Data[0])
			if err != nil {
				return nil, err
			}
		} else {
			err := Patch(database, collection, previousIDs, action.Data[0])
			if err != nil {
				return nil, err
			}
		}
	case "DELETE":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		if action.IDs[0] != "-" {
			err := Delete(database, collection, action.IDs)
			if err != nil {
				return nil, err
			}
		} else {
			err := Delete(database, collection, previousIDs)
			if err != nil {
				return nil, err
			}
		}
	case "DBADD":
		err := CreateDatabase(action.Identifier)
		if err != nil {
			return nil, err
		}
	case "DBDEL":
		err := DeleteDatabase(action.Identifier)
		if err != nil {
			return nil, err
		}
	case "COLADD":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		err := CreateCollection(database, collection, action.Data[0])
		if err != nil {
			return nil, err
		}
	case "COLMOD":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		err := ModifyCollection(database, collection, action.Data[0])
		if err != nil {
			return nil, err
		}
	case "COLDEL":
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		err := COLDELlection(database, collection)
		if err != nil {
			return nil, err
		}
	case "USERADD":
		keys := []string{"username", "password", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return nil, errors.New("Invalid user data, required fields are 'username', 'password', and 'role'")
			}
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(action.Data[0]["password"].(string)), bcrypt.DefaultCost)
		if err != nil {
			return nil, err
		}
		inputData := []map[string]interface{}{{"username": action.Data[0]["username"].(string), "password": string(hash), "role": action.Data[0]["role"].(string)}}
		action := aql.Action{Type: "POST", Identifier: "_auth._users", Data: inputData}
		_, err = ProcessAction(action, []string{})
		if err != nil {
			return nil, err
		}
	case "USERMOD":
		keys := []string{"username", "password", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return nil, errors.New("Invalid user data, required fields are 'username', 'password', and 'role'")
			}
		}
		nodeL := aql.Node{Value: "username"}
		nodeR := aql.Node{Value: action.Data[0]["username"].(string)}
		nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
		getAction := aql.Action{Type: "GET", Identifier: "_auth._users", Filter: nodeC}
		data, err := ProcessAction(getAction, []string{})
		if err != nil {
			return nil, err
		}
		if len(data) != 1 {
			return nil, errors.New("User does not exist")
		}
		data[0]["username"] = action.Data[0]["username"].(string)
		hash, err := bcrypt.GenerateFromPassword([]byte(action.Data[0]["password"].(string)), bcrypt.DefaultCost)
		if err != nil {
			return nil, err
		}
		data[0]["password"] = string(hash)
		data[0]["role"] = action.Data[0]["role"].(string)
		action := aql.Action{Type: "PUT", Identifier: "_auth._users", Data: data}
		_, err = ProcessAction(action, []string{})
		if err != nil {
			return nil, err
		}
	case "USERDEL":
		nodeL := aql.Node{Value: "username"}
		nodeR := aql.Node{Value: action.User}
		nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
		getAction := aql.Action{Type: "GET", Identifier: "_auth._users", Filter: nodeC}
		data, err := ProcessAction(getAction, []string{})
		if err != nil {
			return nil, err
		}
		if len(data) != 1 {
			return nil, errors.New("User does not exist")
		}
		database := "_auth"
		collection := "_users"
		err = Delete(database, collection, []string{data[0][".id"].(string)})
		if err != nil {
			return nil, err
		}
	case "USERGET":
		if action.User == "*" {
			getAction := aql.Action{Type: "GET", Identifier: "_auth._users"}
			data, err := ProcessAction(getAction, []string{})
			for idx, datum := range data {
				delete(datum, "password")
				data[idx] = datum
			}
			return data, err
		} else {
			nodeL := aql.Node{Value: "username"}
			nodeR := aql.Node{Value: action.User}
			nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
			getAction := aql.Action{Type: "GET", Identifier: "_auth._users", Filter: nodeC}
			data, err := ProcessAction(getAction, []string{})
			if err != nil {
				return nil, err
			}
			if len(data) != 1 {
				return nil, errors.New("User does not exist")
			}
			for idx, datum := range data {
				delete(datum, "password")
				data[idx] = datum
			}
			return data, err
		}
	case "PERMITADD":
		keys := []string{"username", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return nil, errors.New("Invalid user data, required fields are 'username' and 'role'")
			}
		}
		inputData := []map[string]interface{}{{"username": action.Data[0]["username"].(string), "role": action.Data[0]["role"].(string)}}
		action := aql.Action{Type: "POST", Identifier: action.Identifier + "._users", Data: inputData}
		_, err := ProcessAction(action, []string{})
		if err != nil {
			return nil, err
		}
	case "PERMITMOD":
		keys := []string{"username", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return nil, errors.New("Invalid user data, required fields are 'username', 'password', and 'role'")
			}
		}
		nodeL := aql.Node{Value: "username"}
		nodeR := aql.Node{Value: action.Data[0]["username"].(string)}
		nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
		getAction := aql.Action{Type: "GET", Identifier: action.Identifier + "._users", Filter: nodeC}
		data, err := ProcessAction(getAction, []string{})
		if err != nil {
			return nil, err
		}
		if len(data) != 1 {
			return nil, errors.New("User does not exist")
		}
		data[0]["username"] = action.Data[0]["username"].(string)
		data[0]["role"] = action.Data[0]["role"].(string)
		action := aql.Action{Type: "PUT", Identifier: action.Identifier + "._users", Data: data}
		_, err = ProcessAction(action, []string{})
		if err != nil {
			return nil, err
		}
	case "PERMITDEL":
		nodeL := aql.Node{Value: "username"}
		nodeR := aql.Node{Value: action.User}
		nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
		getAction := aql.Action{Type: "GET", Identifier: action.Identifier + "._users", Filter: nodeC}
		data, err := ProcessAction(getAction, []string{})
		if err != nil {
			return nil, err
		}
		if len(data) != 1 {
			return nil, errors.New("User does not exist")
		}
		database := action.Identifier
		collection := "_users"
		err = Delete(database, collection, []string{data[0][".id"].(string)})
		if err != nil {
			return nil, err
		}
	case "PERMITGET":
		if action.User == "*" {
			getAction := aql.Action{Type: "GET", Identifier: action.Identifier + "._users"}
			data, err := ProcessAction(getAction, []string{})
			return data, err
		} else {
			nodeL := aql.Node{Value: "username"}
			nodeR := aql.Node{Value: action.User}
			nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
			getAction := aql.Action{Type: "GET", Identifier: action.Identifier + "._users", Filter: nodeC}
			data, err := ProcessAction(getAction, []string{})
			if err != nil {
				return nil, err
			}
			if len(data) != 1 {
				return nil, errors.New("User does not exist")
			}
			return data, err
		}
	}
	return nil, nil
}

func doOrderASC(in []map[string]interface{}, key string) []map[string]interface{} {
	if len(in) > 0 {
		if _, ok := in[0][key].(string); ok {
			sort.Slice(in, func(i, j int) bool { return in[i][key].(string) < in[j][key].(string) })
		} else if _, ok := in[0][key].(int); ok {
			sort.Slice(in, func(i, j int) bool { return in[i][key].(int) < in[j][key].(int) })
		} else if _, ok := in[0][key].(float64); ok {
			sort.Slice(in, func(i, j int) bool { return in[i][key].(float64) < in[j][key].(float64) })
		} else if _, ok := in[0][key].(bool); ok {
			sort.Slice(in, func(i, j int) bool { return boolToInt(in[i][key].(bool)) < boolToInt(in[j][key].(bool)) })
		}
	}
	return in
}

func doOrderDSC(in []map[string]interface{}, key string) []map[string]interface{} {
	if len(in) > 0 {
		if _, ok := in[0][key].(string); ok {
			sort.Slice(in, func(i, j int) bool { return in[i][key].(string) > in[j][key].(string) })
		} else if _, ok := in[0][key].(int); ok {
			sort.Slice(in, func(i, j int) bool { return in[i][key].(int) > in[j][key].(int) })
		} else if _, ok := in[0][key].(float64); ok {
			sort.Slice(in, func(i, j int) bool { return in[i][key].(float64) > in[j][key].(float64) })
		} else if _, ok := in[0][key].(bool); ok {
			sort.Slice(in, func(i, j int) bool { return boolToInt(in[i][key].(bool)) > boolToInt(in[j][key].(bool)) })
		}
	}
	return in
}

func doAnd(A, B []string) []string {
	sort.Strings(A)
	sort.Strings(B)
	out := make([]string, 0)
	for idx, val := range A {
		if len(B) == 0 {
			break
		}
		for B[0] < val {
			B = B[1:]
			if len(B) == 0 {
				break
			}
		}
		if len(B) > 0 {
			if B[0] == A[idx] {
				out = append(out, val)
			}
		}
	}
	out = utils.RemoveDuplicateValues(out)
	return out
}

func doOr(A, B []string) []string {
	sort.Strings(A)
	sort.Strings(B)
	out := make([]string, 0)
	for idx, val := range A {
		if len(B) == 0 {
			out = append(out, A[idx:]...)
			break
		}
		for B[0] < val {
			out = append(out, B[0])
			B = B[1:]
			if len(B) == 0 {
				out = append(out, A[idx:]...)
				break
			}
		}
		if len(B) > 0 {
			if B[0] != val {
				out = append(out, val)
			}
		}
	}
	out = append(out, B...)
	out = utils.RemoveDuplicateValues(out)
	return out
}

func doNot(A, B []string) []string {
	sort.Strings(A)
	sort.Strings(B)
	out := make([]string, 0)
	for idx, val := range A {
		if len(B) == 0 {
			out = append(out, A[idx:]...)
			break
		}
		for B[0] < val {
			B = B[1:]
			if len(B) == 0 {
				out = append(out, A[idx:]...)
				break
			}
		}
		if len(B) > 0 {
			if B[0] != val {
				out = append(out, val)
			}
		}
	}
	out = utils.RemoveDuplicateValues(out)
	return out
}

func doXor(A, B []string) []string {
	sort.Strings(A)
	sort.Strings(B)
	out := make([]string, 0)
	for idx, val := range A {
		if len(B) == 0 {
			out = append(out, A[idx:]...)
			break
		}
		for B[0] < val {
			out = append(out, B[0])
			B = B[1:]
			if len(B) == 0 {
				out = append(out, A[idx:]...)
				break
			}
		}
		if len(B) > 0 {
			if B[0] == val {
				B = B[1:]
			} else {
				out = append(out, val)
			}
		} else {
			out = append(out, val)
		}
	}
	out = utils.RemoveDuplicateValues(out)
	return out
}

func doBoolComparison(left, right bool, operator string) bool {
	switch operator {
	case "=":
		return left == right
	case "!=":
		return left != right
	}
	return false
}

func doFloatComparison(left, right float64, operator string) bool {
	switch operator {
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case "=":
		return left == right
	case "!=":
		return left != right
	}
	return false
}

func doIntComparison(left, right int, operator string) bool {
	switch operator {
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case "=":
		return left == right
	case "!=":
		return left != right
	}
	return false
}

func doStringComparison(left, right string, operator string) bool {
	switch operator {
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case "=":
		return left == right
	case "!=":
		return left != right
	}
	return false
}

func boolToInt(boolVal bool) int {
	if boolVal {
		return 1
	}
	return 0
}

func doFilterBool(database, collection, key string, node aql.Node) ([]string, error) {
	filePath := filepath.Join(config.Config.IndexDir, database, collection, key)
	stringValues, _ := filePathWalkDir(filePath)
	values := make(map[bool]string, 0)
	keys := make([]bool, 0)
	for _, value := range stringValues {
		boolVal, _ := strconv.ParseBool(filepath.Base(value))
		values[boolVal] = value
		keys = append(keys, boolVal)
	}
	sort.Slice(keys, func(i, j int) bool { return boolToInt(keys[i]) < boolToInt(keys[j]) })
	output := make([]string, 0)
	compVal, _ := strconv.ParseBool(node.Right.Value)
	for _, key := range keys {
		if doBoolComparison(key, compVal, node.Value) {
			dat, _ := os.ReadFile(values[key])
			ids := strings.Split(string(dat), "\n")
			ids = ids[:len(ids)-1]
			output = append(output, ids...)
		}
	}
	return output, nil
}

func doFilterFloat(database, collection, key string, node aql.Node) ([]string, error) {
	filePath := filepath.Join(config.Config.IndexDir, database, collection, key)
	stringValues, _ := filePathWalkDir(filePath)
	values := make(map[float64]string, 0)
	keys := make([]float64, 0)
	for _, value := range stringValues {
		floatVal, _ := strconv.ParseFloat(filepath.Base(value), 64)
		values[floatVal] = value
		keys = append(keys, floatVal)
	}
	sort.Float64s(keys)
	output := make([]string, 0)
	compVal, _ := strconv.ParseFloat(node.Right.Value, 64)
	for _, key := range keys {
		if doFloatComparison(key, compVal, node.Value) {
			dat, _ := os.ReadFile(values[key])
			ids := strings.Split(string(dat), "\n")
			ids = ids[:len(ids)-1]
			output = append(output, ids...)
		}
	}
	return output, nil
}

func doFilterInt(database, collection, key string, node aql.Node) ([]string, error) {
	filePath := filepath.Join(config.Config.IndexDir, database, collection, key)
	stringValues, _ := filePathWalkDir(filePath)
	values := make(map[int]string, 0)
	keys := make([]int, 0)
	for _, value := range stringValues {
		intVal, _ := strconv.Atoi(filepath.Base(value))
		values[intVal] = value
		keys = append(keys, intVal)
	}
	sort.Ints(keys)
	output := make([]string, 0)
	compVal, _ := strconv.Atoi(node.Right.Value)
	for _, key := range keys {
		if doIntComparison(key, compVal, node.Value) {
			dat, _ := os.ReadFile(values[key])
			ids := strings.Split(string(dat), "\n")
			ids = ids[:len(ids)-1]
			output = append(output, ids...)
		}
	}
	return output, nil
}

func doFilterString(database, collection, key string, node aql.Node) ([]string, error) {
	filePath := filepath.Join(config.Config.IndexDir, database, collection, key)
	stringValues, _ := filePathWalkDir(filePath)
	values := make(map[string]string, 0)
	keys := make([]string, 0)
	for _, value := range stringValues {
		stringVal := filepath.Base(value)
		values[stringVal] = value
		keys = append(keys, stringVal)
	}
	sort.Strings(keys)
	output := make([]string, 0)
	compVal := node.Right.Value
	for _, key := range keys {
		if doStringComparison(key, compVal, node.Value) {
			dat, _ := os.ReadFile(values[key])
			ids := strings.Split(string(dat), "\n")
			ids = ids[:len(ids)-1]
			output = append(output, ids...)
		}
	}
	return output, nil
}

func doFilter(database, collection string, node aql.Node) ([]string, error) {
	key := node.Left.Value
	switch schema.Schema.Databases[database].Collections[collection].Types[key] {
	case "BOOL":
		out, err := doFilterBool(database, collection, key, node)
		return out, err
	case "INT":
		out, err := doFilterInt(database, collection, key, node)
		return out, err
	case "FLOAT":
		out, err := doFilterFloat(database, collection, key, node)
		return out, err
	case "STRING":
		out, err := doFilterString(database, collection, key, node)
		return out, err
	}
	return []string{}, nil
}

func ProcessFilter(database, collection string, node aql.Node) ([]string, error) {
	switch node.Value {
	case ">":
		output, err := doFilter(database, collection, node)
		return output, err
	case ">=":
		output, err := doFilter(database, collection, node)
		return output, err
	case "=":
		output, err := doFilter(database, collection, node)
		return output, err
	case "<":
		output, err := doFilter(database, collection, node)
		return output, err
	case "<=":
		output, err := doFilter(database, collection, node)
		return output, err
	case "!=":
		output, err := doFilter(database, collection, node)
		return output, err
	case "AND":
		left, err := ProcessFilter(database, collection, *node.Left)
		if err != nil {
			return nil, err
		}
		right, err := ProcessFilter(database, collection, *node.Right)
		if err != nil {
			return nil, err
		}
		output := doAnd(left, right)
		return output, nil
	case "OR":
		left, err := ProcessFilter(database, collection, *node.Left)
		if err != nil {
			return nil, err
		}
		right, err := ProcessFilter(database, collection, *node.Right)
		if err != nil {
			return nil, err
		}
		output := doOr(left, right)
		return output, nil
	case "NOT":
		left, err := index.All(database, collection)
		right, err := ProcessFilter(database, collection, *node.Right)
		if err != nil {
			return nil, err
		}
		output := doNot(left, right)
		return output, nil
	case "XOR":
		left, err := ProcessFilter(database, collection, *node.Left)
		if err != nil {
			return nil, err
		}
		right, err := ProcessFilter(database, collection, *node.Right)
		if err != nil {
			return nil, err
		}
		output := doXor(left, right)
		return output, nil
	}
	return nil, nil
}

func filePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func CreateDatabase(database string) error {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return err
	}
	freespace.FreeSpace.Databases[database] = freespace.FreeSpaceDatabase{}
	schema.Schema.Databases[database] = schema.SchemaDatabase{}
	freespace.WriteFreeSpace()
	schema.WriteSchema()
	if database != "_auth" {
		CreateDBAuthCollection(database)
	}
	return nil
}

func DeleteDatabase(database string) error {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	if err := os.RemoveAll(dataPath); err != nil {
		return err
	}
	if err := os.RemoveAll(indexPath); err != nil {
		return err
	}
	delete(freespace.FreeSpace.Databases, database)
	delete(schema.Schema.Databases, database)
	return nil
}

func CreateCollection(database, collection string, newSchema map[string]interface{}) error {
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

func ModifyCollection(database, collection string, newSchema map[string]interface{}) error {
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

func COLDELlection(database, collection string) error {
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

func CreateDBAuthCollection(database string) error {
	CreateCollection(database, "_users", map[string]interface{}{"username": "STRING", "role": "STRING"})
	inputData := []map[string]interface{}{{"username": "ceres", "role": "ADMIN"}}
	action := aql.Action{Type: "POST", Identifier: database + "._users", Data: inputData}
	_, err := ProcessAction(action, []string{})
	if err != nil {
		return err
	}
	return nil
}
