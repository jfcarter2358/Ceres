package aql

import (
	"ceresdb/auth"
	"ceresdb/constants"
	"ceresdb/query"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
	"strings"
)

func processGet(token Token, u auth.User) (interface{}, error) {
	switch token.NounType {
	case constants.NOUN_COLLECTION:
		return processGetCollection(token, u)
	case constants.NOUN_DATABASE:
		return processGetDatabase(token, u)
	case constants.NOUN_USER:
		return processGetUser(token, u)
	case constants.NOUN_SCHEMA:
		return processGetSchema(token, u)
	case constants.NOUN_RECORD:
		return processGetRecord(token, u)
	}
	return nil, fmt.Errorf("invalid query: unknown noun type %s", token.NounType)
}

func processGetCollection(token Token, u auth.User) (interface{}, error) {
	out, err := query.ListCollections(token.Location, u)
	if err != nil {
		return nil, err
	}
	if token.Count {
		return []interface{}{map[string]int{"count": len(out.([]interface{}))}}, nil
	}
	return out, nil
}

func processGetDatabase(token Token, u auth.User) (interface{}, error) {
	out := query.ListDatabases(u)
	if token.Count {
		return []interface{}{map[string]int{"count": len(out.([]interface{}))}}, nil
	}
	return out, nil
}

func processGetRecord(token Token, u auth.User) (interface{}, error) {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	f := token.Filter
	o := token.Order
	of := token.OrderField

	var out []interface{}
	var err error

	if f != "" {
		// filter is present
		var ff map[string]interface{}
		if err := json.Unmarshal([]byte(f), &ff); err != nil {
			return nil, err
		}
		if strings.HasPrefix(d, constants.COLD_STORAGE_PREFIX) {
			out, err = query.GetRecordColdStorage(d[len(constants.COLD_STORAGE_PREFIX):], c, ff, u)
			if err != nil {
				return nil, err
			}
		} else {
			out, err = query.GetRecordIndex(d, c, ff, u)
			if err != nil {
				return nil, err
			}
		}
	} else {
		if strings.HasPrefix(d, constants.COLD_STORAGE_PREFIX) {
			out, err = query.GetRecordAllColdStorage(d[len(constants.COLD_STORAGE_PREFIX):], c, u)
			if err != nil {
				return nil, err
			}
		} else {
			out, err = query.GetRecordAllIndex(d, c, u)
			if err != nil {
				return nil, err
			}
		}
	}

	switch o {
	case constants.ADJECTIVE_ASCENDING:
		keys := strings.Split(of, ".")
		out, err = utils.SortInterfacesAscending(keys, out)
	case constants.ADJECTIVE_DESCENDING:
		keys := strings.Split(of, ".")
		out, err = utils.SortInterfacesDescending(keys, out)
	}
	if token.Count {
		return []interface{}{map[string]int{"count": len(out)}}, nil
	}

	if token.Limit > 0 && token.Limit < len(out) {
		out = out[:token.Limit]
	}

	if len(token.Output) > 0 {
		for idx, datum := range out {
			temp := map[string]interface{}{}
			for key, val := range datum.(map[string]interface{}) {
				if utils.Contains(token.Output, key) {
					temp[key] = val
				}
			}
			out[idx] = temp
		}
	}
	return out, err
}

func processGetSchema(token Token, u auth.User) (interface{}, error) {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]

	return query.GetSchema(d, c, u)
}

func processGetUser(token Token, u auth.User) (interface{}, error) {
	out, err := query.GetUserAll(u)
	if err != nil {
		return nil, err
	}
	if token.Count {
		return []interface{}{map[string]int{"count": len(out)}}, nil
	}
	return out, nil
}
