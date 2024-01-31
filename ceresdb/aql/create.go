package aql

import (
	"ceresdb/auth"
	"ceresdb/constants"
	"ceresdb/database"
	"ceresdb/index"
	"ceresdb/query"
	"encoding/json"
	"fmt"
)

func processCreate(token Token, u auth.User) error {
	switch token.NounType {
	case constants.NOUN_COLLECTION:
		return processCreateCollection(token, u)
	case constants.NOUN_DATABASE:
		return processCreateDatabase(token, u)
	case constants.NOUN_USER:
		return processCreateUser(token, u)
	}
	return fmt.Errorf("invalid query: unknown noun type %s", token.NounType)
}

func processCreateCollection(token Token, u auth.User) error {
	c := token.Noun
	d := token.Location
	s := token.Additional

	if err := database.AddCollection(d, c, u); err != nil {
		return err
	}
	if err := query.CreateCollection(d, c, u); err != nil {
		return err
	}

	var ss interface{}
	if err := json.Unmarshal([]byte(s), &ss); err != nil {
		return err
	}
	if err := query.BuildSchema(d, c, ss, u); err != nil {
		return err
	}
	return index.BuildIndex(d, c)
}

func processCreateDatabase(token Token, u auth.User) error {
	d := token.Noun

	return query.CreateDatabase(d, u)
}

func processCreateUser(token Token, u auth.User) error {
	un := token.Noun
	p := token.Additional

	return query.CreateUser(un, p, u, false)
}
