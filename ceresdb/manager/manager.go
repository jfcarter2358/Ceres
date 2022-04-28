// manager.go

package manager

import (
	"ceresdb/aql"
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/database"
	"ceresdb/index"
	"ceresdb/permit"
	"ceresdb/record"
	"ceresdb/schema"
	"ceresdb/user"
	"ceresdb/utils"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func ProcessGet(action aql.Action, previousIDs []string, internal bool) ([]map[string]interface{}, error) {
	switch action.Resource {
	case "COLLECTION":
		data, err := collection.Get(action.Identifier)
		if err != nil {
			return nil, err
		}
		if action.Limit > 0 && action.Limit < len(data) {
			data = data[:action.Limit]
		}
		return data, nil
	case "DATABASE":
		data, err := database.Get()
		if err != nil {
			return nil, err
		}
		if action.Limit > 0 && action.Limit < len(data) {
			data = data[:action.Limit]
		}
		return data, nil
	case "PERMIT":
		var ids []string
		var err error
		if action.Filter.Value != "" {
			ids, err = ProcessFilter(action.Identifier, "_users", action.Filter)
		} else {
			ids, err = index.All(action.Identifier, "_users")
		}
		if err != nil {
			return nil, err
		}
		data, err := permit.Get(action.Identifier, ids)
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
	case "RECORD":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		var ids []string
		var err error
		if action.Filter.Value != "" {
			ids, err = ProcessFilter(db, col, action.Filter)
		} else {
			ids, err = index.All(db, col)
		}
		if err != nil {
			return nil, err
		}
		data, err := record.Get(db, col, ids)
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
	case "USER":
		var ids []string
		var err error
		if action.Filter.Value != "" {
			ids, err = ProcessFilter("_auth", "_users", action.Filter)
		} else {
			ids, err = index.All("_auth", "_users")
		}
		if err != nil {
			return nil, err
		}
		data, err := user.Get(ids)
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
		if !internal {
			for idx, datum := range data {
				delete(datum, "password")
				data[idx] = datum
			}
		}
		return data, nil
	}
	return nil, errors.New("Invalid resource type")
}

func ProcessPost(action aql.Action, previousIDs []string) error {
	switch action.Resource {
	case "COLLECTION":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		err := collection.Post(db, col, action.Data[0])
		return err
	case "DATABASE":
		err := database.Post(action.Identifier)
		return err
	case "PERMIT":
		keys := []string{"username", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return errors.New("Invalid user data, required fields are 'username' and 'role'")
			}
		}
		inputData := []map[string]interface{}{{"username": action.Data[0]["username"].(string), "role": action.Data[0]["role"].(string)}}
		err := permit.Post(action.Identifier, inputData)
		return err
	case "RECORD":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		err := record.Post(db, col, action.Data)
		return err
	case "USER":
		keys := []string{"username", "password", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return errors.New("Invalid user data, required fields are 'username', 'password', and 'role'")
			}
		}
		for idx, datum := range action.Data {
			hash, err := bcrypt.GenerateFromPassword([]byte(datum["password"].(string)), bcrypt.DefaultCost)
			if err != nil {
				return err
			}
			action.Data[idx]["password"] = string(hash)
		}
		err := user.Post(action.Data)
		return err
	}
	return errors.New("Invalid resource type")
}

func ProcessPut(action aql.Action, previousIDs []string) error {
	switch action.Resource {
	case "COLLECTION":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		err := collection.Put(db, col, action.Data[0])
		return err
	case "DATABASE":
		err := database.Put()
		return err
	case "PERMIT":
		keys := []string{"username", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return errors.New("Invalid user data, required fields are 'username', 'password', and 'role'")
			}
		}
		nodeL := aql.Node{Value: "username"}
		nodeR := aql.Node{Value: action.Data[0]["username"].(string)}
		nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
		getAction := aql.Action{Type: "GET", Resource: "PERMIT", Identifier: action.Identifier, Filter: nodeC}
		data, err := ProcessGet(getAction, []string{}, false)
		if err != nil {
			return err
		}
		if len(data) != 1 {
			return errors.New("User does not exist")
		}
		data[0]["username"] = action.Data[0]["username"].(string)
		data[0]["role"] = action.Data[0]["role"].(string)
		err = permit.Put(action.Identifier, data)
		return err
	case "RECORD":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		err := record.Put(db, col, action.Data)
		return err
	case "USER":
		keys := []string{"username", "password", "role"}
		for _, key := range keys {
			if _, ok := action.Data[0][key]; !ok {
				return errors.New("Invalid user data, required fields are 'username', 'password', and 'role'")
			}
		}
		for idx, datum := range action.Data {
			hash, err := bcrypt.GenerateFromPassword([]byte(datum["password"].(string)), bcrypt.DefaultCost)
			if err != nil {
				return err
			}
			action.Data[idx]["password"] = string(hash)
		}
		err := user.Put(action.Data)
		return err
	}
	return errors.New("Invalid resource type")
}

func ProcessPatch(action aql.Action, previousIDs []string) error {
	switch action.Resource {
	case "COLLECTION":
		err := collection.Patch()
		return err
	case "DATABASE":
		err := database.Patch()
		return err
	case "PERMIT":
		err := permit.Patch()
		return err
	case "RECORD":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		if action.IDs[0] != "-" {
			err := record.Patch(db, col, action.IDs, action.Data[0])
			return err
		} else {
			err := record.Patch(db, col, previousIDs, action.Data[0])
			return err
		}
	case "USER":
		err := user.Patch()
		return err
	}
	return errors.New("Invalid resource type")
}

func ProcessDelete(action aql.Action, previousIDs []string) error {
	switch action.Resource {
	case "COLLECTION":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		err := collection.Delete(db, col)
		return err
	case "DATABASE":
		err := database.Delete(action.Identifier)
		return err
	case "PERMIT":
		if action.IDs[0] != "-" {
			err := permit.Delete(action.Identifier, action.IDs)
			return err
		} else {
			err := permit.Delete(action.Identifier, previousIDs)
			return err
		}
	case "RECORD":
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		col := parts[1]
		if action.IDs[0] != "-" {
			err := record.Delete(db, col, action.IDs)
			return err
		} else {
			err := record.Delete(db, col, previousIDs)
			return err
		}
	case "USER":
		if action.IDs[0] != "-" {
			err := user.Delete(action.IDs)
			return err
		} else {
			err := user.Delete(previousIDs)
			return err
		}
	}
	return errors.New("Invalid resource type")
}

func ProcessCount(action aql.Action, previousIDs []string) ([]map[string]interface{}, error) {
	output := []map[string]interface{}{{"count": len(previousIDs)}}
	return output, nil
}

func ProcessAction(action aql.Action, previousIDs []string, internal bool) ([]map[string]interface{}, error) {
	switch action.Type {
	case "GET":
		data, err := ProcessGet(action, previousIDs, internal)
		return data, err
	case "POST":
		err := ProcessPost(action, previousIDs)
		return nil, err
	case "PUT":
		err := ProcessPut(action, previousIDs)
		return nil, err
	case "PATCH":
		err := ProcessPatch(action, previousIDs)
		return nil, err
	case "DELETE":
		err := ProcessDelete(action, previousIDs)
		return nil, err
	case "COUNT":
		data, err := ProcessCount(action, previousIDs)
		return data, err
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
		decodedVal, _ := base64.StdEncoding.DecodeString(filepath.Base(value))
		boolVal, _ := strconv.ParseBool(string(decodedVal))
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
		decodedVal, _ := base64.StdEncoding.DecodeString(filepath.Base(value))
		floatVal, _ := strconv.ParseFloat(string(decodedVal), 64)
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
		decodedVal, _ := base64.StdEncoding.DecodeString(filepath.Base(value))
		intVal, _ := strconv.Atoi(string(decodedVal))
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
		decodedVal, _ := base64.StdEncoding.DecodeString(filepath.Base(value))
		stringVal := string(decodedVal)
		if stringVal == index.EMPTY_FIELD_VALUE {
			stringVal = ""
		}
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
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}
