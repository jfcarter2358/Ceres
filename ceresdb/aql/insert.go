package aql

import (
	"ceresdb/auth"
	"ceresdb/constants"
	"ceresdb/query"
	"encoding/json"
	"fmt"
	"strings"
)

func processInsert(token Token, u auth.User) error {
	switch token.NounType {
	case constants.NOUN_RECORD:
		return processInsertRecord(token, u)
	}
	return fmt.Errorf("invalid query: unknown noun type %s", token.NounType)
}

func processInsertRecord(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]

	var data interface{}
	if err := json.Unmarshal([]byte(token.Noun), &data); err != nil {
		return err
	}
	if l, ok := data.([]interface{}); ok {
		for _, r := range l {
			if err := query.WriteRecord(d, c, r, u, false); err != nil {
				return err
			}
		}
		return nil
	}
	err := query.WriteRecord(d, c, data, u, false)
	return err
}
