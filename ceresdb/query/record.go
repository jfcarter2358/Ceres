package query

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/index"
	"ceresdb/logger"
	"ceresdb/record"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
	"strings"
)

func DeleteRecordIndex(d, c string, filter map[string]interface{}, u auth.User) error {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	ids, err := doFilterIndex(d, c, []string{}, filter, constants.FILTER_AND)
	if err != nil {
		return err
	}
	for _, id := range ids {
		err := record.Delete(d, c, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteRecordColdStorage(d, c string, filter map[string]interface{}, u auth.User) error {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	ids := []string{}
	for f, fi := range collection.Collections[d][c].Files {
		if fi.Length > 0 {
			path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, f)
			lines, err := record.ReadDataFile(path)
			if err != nil {
				return err
			}
			for _, line := range lines {
				if len(line) > 0 {
					var val map[string]interface{}
					if err := json.Unmarshal([]byte(line), &val); err != nil {
						return err
					}
					valid, err := doFilterIDs(d, c, []string{}, filter, val)
					if err != nil {
						return err
					}
					if valid {
						ids = append(ids, val[constants.ID_KEY].(string))
					}
				}
			}
		}
	}
	for _, id := range ids {
		if err := record.Delete(d, c, id); err != nil {
			return err
		}
	}
	return nil
}

func GetRecordIndex(d, c string, filter map[string]interface{}, u auth.User) ([]interface{}, error) {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_READ, u); err != nil {
		return nil, err
	}
	logger.Tracef("", "filtering on %v", filter)
	ids, err := doFilterIndex(d, c, []string{}, filter, constants.FILTER_AND)
	if err != nil {
		return nil, err
	}

	logger.Tracef("", "got ids %v", ids)
	out := []interface{}{}
	for _, id := range ids {
		r, err := record.Get(d, c, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func GetRecordColdStorage(d, c string, filter map[string]interface{}, u auth.User) ([]interface{}, error) {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return nil, err
	}
	ids := []string{}
	for f, fi := range collection.Collections[d][c].Files {
		if fi.Length > 0 {
			path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, f)
			lines, err := record.ReadDataFile(path)
			if err != nil {
				return nil, err
			}
			for _, line := range lines {
				if len(line) > 0 {
					var val map[string]interface{}
					if err := json.Unmarshal([]byte(line), &val); err != nil {
						return nil, err
					}
					valid, err := doFilterIDs(d, c, []string{}, filter, val)
					if err != nil {
						return nil, err
					}
					if valid {
						ids = append(ids, val[constants.ID_KEY].(string))
					}
				}
			}
		}
	}
	out := []interface{}{}
	for _, id := range ids {
		r, err := record.Get(d, c, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func GetRecordAllIndex(d, c string, u auth.User) ([]interface{}, error) {
	logger.Tracef("", "Checking auth")
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_READ, u); err != nil {
		return nil, err
	}
	logger.Tracef("", "Get all index")
	out, err := record.GetAllIndex(d, c)
	return out, err
}

func GetRecordAllColdStorage(d, c string, u auth.User) ([]interface{}, error) {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_READ, u); err != nil {
		return nil, err
	}
	out, err := record.GetAllColdStorage(d, c)
	return out, err
}

func UpdateRecordIndex(d, c string, filter map[string]interface{}, data interface{}, u auth.User) error {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	ids, err := doFilterIndex(d, c, []string{}, filter, constants.FILTER_AND)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := record.Update(d, c, id, data); err != nil {
			return err
		}
	}
	return nil
}

func UpdateRecordColdStorage(d, c string, filter map[string]interface{}, data interface{}, u auth.User) error {
	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	ids := []string{}
	for f, fi := range collection.Collections[d][c].Files {
		if fi.Length > 0 {
			path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, c, f)
			lines, err := record.ReadDataFile(path)
			if err != nil {
				return err
			}
			for _, line := range lines {
				if len(line) > 0 {
					var val map[string]interface{}
					if err := json.Unmarshal([]byte(line), &val); err != nil {
						return err
					}
					valid, err := doFilterIDs(d, c, []string{}, filter, val)
					if err != nil {
						return err
					}
					if valid {
						ids = append(ids, val[constants.ID_KEY].(string))
					}
				}
			}
		}
	}
	for _, id := range ids {
		if err := record.Update(d, c, id, data); err != nil {
			return err
		}
	}
	return nil
}

func WriteRecord(d, c string, data interface{}, u auth.User, bypass bool) error {
	if !bypass {
		if err := collection.VerifyAuth(d, c, constants.PERMISSION_WRITE, u); err != nil {
			return err
		}
	}
	return record.Write(d, c, data)
}

func doFilterIndex(d, c string, keys []string, filter map[string]interface{}, mode string) ([]string, error) {
	out := []string{}
	if mode == constants.FILTER_NOT {
		out = index.IndexIDs[d][c]
	}
	for key, val := range filter {
		// var err error
		switch key {
		case constants.FILTER_AND:
			temp, err := doFilterIndex(d, c, keys, val.(map[string]interface{}), constants.FILTER_AND)
			if err != nil {
				return nil, err
			}
			out = temp
		case constants.FILTER_OR:
			temp, err := doFilterIndex(d, c, keys, val.(map[string]interface{}), constants.FILTER_OR)
			if err != nil {
				return nil, err
			}
			out = temp

			// for k, v := range val.(map[string]interface{}) {
			// 	temp, err := doFilterIndex(d, c, append(keys, k), v.(map[string]interface{}), mode)
			// 	if err != nil {
			// 		return nil, err
			// 	}
			// 	out = temp
			// }
		case constants.FILTER_NOT:
			// out = index.IndexIDs[d][c]
			temp, err := doFilterIndex(d, c, keys, val.(map[string]interface{}), constants.FILTER_NOT)
			if err != nil {
				return nil, err
			}
			out = temp
			// for k, v := range val.(map[string]interface{}) {
			// 	temp, err := doFilterIndex(d, c, append(keys, k), v.(map[string]interface{}), constants.FILTER_NOT)
			// 	if err != nil {
			// 		return nil, err
			// 	}
			// out = utils.NotLists(out, temp)
			// }
		case constants.FILTER_GT, constants.FILTER_GTE, constants.FILTER_EQ, constants.FILTER_LTE, constants.FILTER_LT:
			for k, v := range val.(map[string]interface{}) {
				temp, err := doCompare(d, c, k, key, keys, v)
				if err != nil {
					return nil, err
				}
				if len(out) == 0 {
					out = temp
				} else {
					out = utils.AndLists(out, temp)
				}
			}
		default:
			if m, ok := val.(map[string]interface{}); ok {
				// for k, _ := range m {
				// if vv, ok := v.(map[string]interface{}); ok {
				// 	temp, err := doFilterIndex(d, c, append(keys, k), vv)
				// 	if err != nil {
				// 		return nil, err
				// 	}
				// 	if len(out) == 0 {
				// 		out = temp
				// 	} else {
				// 		out = utils.AndLists(out, temp)
				// 	}
				// 	continue
				// }
				// return nil, fmt.Errorf("filter value %v of type %T at %s is of invalid type, want map[string]interface{}", v, v, strings.Join(append(keys, k), "."))
				temp, err := doFilterIndex(d, c, append(keys, key), m, constants.FILTER_AND)
				// temp, err := doCompare(d, c, k, constants.FILTER_EQ, append(keys, key), v)
				if err != nil {
					return nil, err
				}
				switch mode {
				case constants.FILTER_AND:
					if len(out) == 0 {
						out = temp
					} else {
						out = utils.AndLists(out, temp)
					}
				case constants.FILTER_OR:
					logger.Debugf("", "Doing or comparison between %v and %v", out, temp)
					out = utils.OrLists(out, temp)
				case constants.FILTER_NOT:
					logger.Debugf("", "Doing not comparison between %v and %v", out, temp)
					out = utils.NotLists(out, temp)
				}
				// }
			} else if l, ok := val.([]interface{}); ok {
				for _, el := range l {
					dict := map[string]interface{}{key: el}
					temp, err := doFilterIndex(d, c, keys, dict, constants.FILTER_AND)
					if err != nil {
						return nil, err
					}
					out = utils.OrLists(out, temp)
				}
			} else {
				temp, err := index.RetrieveFromIndex(d, c, append(keys, key), val)
				if err != nil {
					return nil, err
				}
				switch mode {
				case constants.FILTER_AND:
					if len(out) == 0 {
						out = temp
					} else {
						out = utils.AndLists(out, temp)
					}
				case constants.FILTER_OR:
					logger.Debugf("", "Doing or comparison 1 between %v and %v", out, temp)
					out = utils.OrLists(out, temp)
				case constants.FILTER_NOT:
					logger.Debugf("", "Doing not comparison 1 between %v and %v", out, temp)
					out = utils.NotLists(out, temp)
				}
			}
		}
	}
	return out, nil
}

func doFilterIDs(d, c string, keys []string, filter map[string]interface{}, element interface{}) (bool, error) {
	for key, val := range filter {
		switch key {
		case constants.FILTER_AND:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
				}
				for k, v := range m {
					valid, err := doFilterIDs(d, c, append(keys, k), v.(map[string]interface{}), e[k])
					if err != nil || !valid {
						return false, err
					}
				}
				return true, nil
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
		case constants.FILTER_OR:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
				}
				for k, v := range m {
					valid, err := doFilterIDs(d, c, append(keys, k), v.(map[string]interface{}), e[k])
					if err != nil {
						return false, err
					}
					if valid {
						return true, nil
					}
				}
				return false, nil
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
		case constants.FILTER_NOT:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
				}
				for k, v := range m {
					valid, err := doFilterIDs(d, c, append(keys, k), v.(map[string]interface{}), e[k])
					if err != nil || valid {
						return false, err
					}
				}
				return true, nil
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
		case constants.FILTER_GT:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
				}
				for k, v := range m {
					if e[k].(float64) > v.(float64) {
						return true, nil
					}
					return false, nil
				}
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
		case constants.FILTER_GTE:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
				}
				for k, v := range m {
					if e[k].(float64) >= v.(float64) {
						return true, nil
					}
					return false, nil
				}
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
		case constants.FILTER_LTE:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
				}
				for k, v := range m {
					if e[k].(float64) <= v.(float64) {
						return true, nil
					}
					return false, nil
				}
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
		case constants.FILTER_LT:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
				}
				for k, v := range m {
					if e[k].(float64) < v.(float64) {
						return true, nil
					}
					return false, nil
				}
			}
			return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
		default:
			if m, ok := val.(map[string]interface{}); ok {
				var e map[string]interface{}
				if el, ok := element.(map[string]interface{}); ok {
					e = el
				} else {
					return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(keys, "."))
				}
				for k, v := range m {
					valid, err := doFilterIDs(d, c, append(keys, k), v.(map[string]interface{}), e[k])
					if err != nil || !valid {
						return false, err
					}
				}
				return true, nil
			} else if l, ok := val.([]interface{}); ok {
				for _, e := range l {
					f := map[string]interface{}{key: e}
					valid, err := doFilterIDs(d, c, keys, f, element)
					if err != nil {
						return false, err
					}
					if valid {
						return true, nil
					}
				}
				return false, nil
			} else {
				if e, ok := element.(map[string]interface{}); ok {
					if e[key] == val {
						return true, nil
					}
					return false, nil
				}
				return false, fmt.Errorf("value at %s is not valid type, must be of map[string]interface{}", strings.Join(append(keys, key), "."))
			}
		}
	}
	return false, nil
}

func doCompare(d, c, k, op string, keys []string, b interface{}) ([]string, error) {
	logger.Tracef("", "Doing compare")
	dType, vals, err := index.RetrieveValsFromIndex(d, c, append(keys, k))
	logger.Tracef("", "Got datatype %s for vals %v", dType, vals)
	if err != nil {
		return nil, err
	}
	out := []string{}
	switch dType {
	case constants.DATATYPE_FLOAT:
		for a, ids := range vals.(map[float64][]string) {
			out = utils.OrLists(out, opCompareFloat(op, a, b.(float64), out, ids))
		}
	case constants.DATATYPE_INT:
		for a, ids := range vals.(map[int][]string) {
			out = utils.OrLists(out, opCompareFloat(op, float64(a), b.(float64), out, ids))
		}
	case constants.DATATYPE_STRING:
		for a, ids := range vals.(map[string][]string) {
			out = utils.OrLists(out, opCompareString(op, a, b.(string), out, ids))
		}
	default:
		return nil, fmt.Errorf("%s operation does not support datatype %s", op, dType)
	}
	logger.Tracef("", "compare out 2: %v", out)
	return out, nil
}

func opCompareFloat(op string, a, b float64, o, i []string) []string {
	logger.Tracef("", "Comparing %f and %f with operation %s", a, b, op)
	logger.Tracef("", "inbound lists: %v, %v", o, i)
	switch op {
	case constants.FILTER_GT:
		if a > b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_GTE:
		if a >= b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_EQ:
		if a == b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_LTE:
		if a <= b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_LT:
		if a < b {
			return utils.OrLists(o, i)
		}
	}
	return []string{}
}

func opCompareString(op, a, b string, o, i []string) []string {
	logger.Tracef("", "Comparing %s and %s with operation %s", a, b, op)
	logger.Tracef("", "inbound lists: %v, %v", o, i)
	switch op {
	case constants.FILTER_GT:
		if a > b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_GTE:
		if a >= b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_EQ:
		if a == b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_LTE:
		if a <= b {
			return utils.OrLists(o, i)
		}
	case constants.FILTER_LT:
		if a < b {
			return utils.OrLists(o, i)
		}
	}
	return []string{}
}
