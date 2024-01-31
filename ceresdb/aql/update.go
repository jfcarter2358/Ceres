package aql

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/constants"
	"ceresdb/query"
	"encoding/json"
	"fmt"
	"strings"
)

func processUpdate(token Token, u auth.User) error {
	switch token.NounType {
	case constants.NOUN_GROUP:
		return processUpdateGroup(token, u)
	case constants.NOUN_USER:
		return processUpdateUser(token, u)
	case constants.NOUN_SCHEMA:
		return processUpdateRole(token, u)
	case constants.NOUN_RECORD:
		return processUpdateRecord(token, u)
	}
	return fmt.Errorf("invalid query: unknown noun type %s", token.NounType)
}

func processUpdateGroup(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	g := token.Noun
	p := token.Additional

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.AddGroupAuth(d, c, g, p)
}

func processUpdateRecord(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	f := token.Filter
	j := token.Additional

	var ff map[string]interface{}
	if err := json.Unmarshal([]byte(f), &ff); err != nil {
		return err
	}
	var dd interface{}
	if err := json.Unmarshal([]byte(j), &dd); err != nil {
		return err
	}
	if strings.HasPrefix(d, constants.COLD_STORAGE_PREFIX) {
		return query.UpdateRecordColdStorage(d[len(constants.COLD_STORAGE_PREFIX):], c, ff, dd, u)
	}
	return query.UpdateRecordIndex(d, c, ff, dd, u)
}

func processUpdateRole(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	r := token.Noun
	p := token.Additional

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.AddRoleAuth(d, c, r, p)
}

func processUpdateUser(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	un := token.Noun
	p := token.Additional

	if token.Location != "" {
		if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
			return err
		}
		return collection.AddUserAuth(d, c, un, p)
	}
	if err := collection.VerifyAuth(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return query.UpdateUserPassword(un, p, u)
}
