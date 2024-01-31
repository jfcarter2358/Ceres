package aql

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/constants"
	"ceresdb/database"
	"ceresdb/query"
	"ceresdb/utils"
	"fmt"
	"strings"
)

func processAdd(token Token, u auth.User) error {
	switch token.NounType {
	case constants.NOUN_GROUP:
		switch token.LocationType {
		case constants.NOUN_USER:
			return processAddGroupUser(token, u)
		case constants.NOUN_COLLECTION:
			return processAddGroupCollection(token, u)
		case constants.NOUN_DATABASE:
			return processAddGroupDatabase(token, u)
		default:
			return fmt.Errorf("invalid query: invalid location type for group add: %s", token.LocationType)
		}
	case constants.NOUN_ROLE:
		switch token.LocationType {
		case constants.NOUN_USER:
			return processAddRoleUser(token, u)
		case constants.NOUN_COLLECTION:
			return processAddRoleCollection(token, u)
		case constants.NOUN_DATABASE:
			return processAddRoleDatabase(token, u)
		default:
			return fmt.Errorf("invalid query: invalid location type for role add: %s", token.LocationType)
		}
	case constants.NOUN_USER:
		switch token.LocationType {
		case constants.NOUN_COLLECTION:
			return processAddUserCollection(token, u)
		case constants.NOUN_DATABASE:
			return processAddUserDatabase(token, u)
		default:
			return fmt.Errorf("invalid query: invalid location type for user add: %s", token.LocationType)
		}
	}
	return fmt.Errorf("invalid query: unknown noun type %s", token.NounType)
}

func processAddGroupUser(token Token, u auth.User) error {
	g := token.Noun
	un := token.Location

	uu, err := query.GetUser(un, u)
	if err != nil {
		return err
	}
	uu.Groups = append(uu.Groups, g)
	uu.Groups = utils.RemoveDuplicateValues(uu.Groups)
	if err := query.UpdateUser(un, uu, u); err != nil {
		return err
	}

	return nil
}

func processAddGroupCollection(token Token, u auth.User) error {
	g := token.Noun
	p := token.Additional

	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.AddGroupAuth(d, c, g, p)
}

func processAddGroupDatabase(token Token, u auth.User) error {
	g := token.Noun
	p := token.Additional
	d := token.Location

	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.AddGroupAuth(d, g, p)
}

func processAddRoleUser(token Token, u auth.User) error {
	r := token.Noun
	un := token.Location

	uu, err := query.GetUser(un, u)
	if err != nil {
		return err
	}
	uu.Roles = append(uu.Roles, r)
	uu.Roles = utils.RemoveDuplicateValues(uu.Roles)
	if err := query.UpdateUser(un, uu, u); err != nil {
		return err
	}

	return nil
}

func processAddRoleCollection(token Token, u auth.User) error {
	r := token.Noun
	p := token.Additional

	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.AddRoleAuth(d, c, r, p)
}

func processAddRoleDatabase(token Token, u auth.User) error {
	r := token.Noun
	p := token.Additional
	d := token.Location

	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.AddRoleAuth(d, r, p)
}

func processAddUserCollection(token Token, u auth.User) error {
	un := token.Noun
	p := token.Additional

	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.AddRoleAuth(d, c, un, p)
}

func processAddUserDatabase(token Token, u auth.User) error {
	un := token.Noun
	p := token.Additional
	d := token.Location

	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.AddRoleAuth(d, un, p)
}
