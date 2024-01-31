package aql

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/constants"
	"ceresdb/database"
	"ceresdb/query"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
	"strings"
)

func processDelete(token Token, u auth.User) error {
	switch token.NounType {
	case constants.NOUN_COLLECTION:
		return processDeleteCollection(token, u)
	case constants.NOUN_DATABASE:
		return processDeleteDatabase(token, u)
	case constants.NOUN_USER:
		switch token.LocationType {
		case constants.NOUN_COLLECTION:
			return processDeleteUserCollection(token, u)
		case constants.NOUN_DATABASE:
			return processDeleteUserDatabase(token, u)
		default:
			return processDeleteUser(token, u)
		}
	case constants.NOUN_GROUP:
		switch token.LocationType {
		case constants.NOUN_USER:
			return processDeleteGroupUser(token, u)
		case constants.NOUN_COLLECTION:
			return processDeleteGroupCollection(token, u)
		case constants.NOUN_DATABASE:
			return processDeleteGroupDatabase(token, u)
		default:
			return fmt.Errorf("invalid query: invalid location type for group delete: %s", token.LocationType)
		}
	case constants.NOUN_ROLE:
		switch token.LocationType {
		case constants.NOUN_USER:
			return processDeleteRoleUser(token, u)
		case constants.NOUN_COLLECTION:
			return processDeleteRoleCollection(token, u)
		case constants.NOUN_DATABASE:
			return processDeleteRoleDatabase(token, u)
		default:
			return fmt.Errorf("invalid query: invalid location type for role delete: %s", token.LocationType)
		}
	case constants.NOUN_RECORD:
		return processDeleteRecord(token, u)
	}
	return fmt.Errorf("invalid query: unknown noun type %s", token.NounType)
}

func processDeleteCollection(token Token, u auth.User) error {
	c := token.Noun
	d := token.Location

	if err := database.DeleteCollection(d, c, u); err != nil {
		return err
	}
	return query.DeleteCollection(d, c, u)
}

func processDeleteDatabase(token Token, u auth.User) error {
	d := token.Noun

	for _, col := range database.Databases[d].Collections {
		if err := collection.Delete(d, col); err != nil {
			return err
		}
	}
	return query.DeleteDatabase(d, u)
}

func processDeleteRecord(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	f := token.Filter

	var ff map[string]interface{}
	if err := json.Unmarshal([]byte(f), &ff); err != nil {
		return err
	}
	if strings.HasPrefix(d, constants.COLD_STORAGE_PREFIX) {
		return query.DeleteRecordIndex(d[len(constants.COLD_STORAGE_PREFIX):], c, ff, u)
	}
	return query.DeleteRecordIndex(d, c, ff, u)
}

func processDeleteUser(token Token, u auth.User) error {
	un := token.Noun

	return query.DeleteUser(un, u)
}

func processDeleteGroupUser(token Token, u auth.User) error {
	g := token.Noun
	un := token.Location

	uu, err := query.GetUser(un, u)
	if err != nil {
		return err
	}
	uu.Groups = utils.Remove(uu.Groups, g)
	if err := query.UpdateUser(un, uu, u); err != nil {
		return err
	}
	return nil
}

func processDeleteGroupCollection(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	g := token.Noun

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.DeleteGroupAuth(d, c, g)
}

func processDeleteGroupDatabase(token Token, u auth.User) error {
	d := token.Location
	g := token.Noun

	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.DeleteGroupAuth(d, g)
}

func processDeleteRoleUser(token Token, u auth.User) error {
	r := token.Noun
	un := token.Location

	uu, err := query.GetUser(un, u)
	if err != nil {
		return err
	}
	uu.Roles = utils.Remove(uu.Roles, r)
	if err := query.UpdateUser(un, uu, u); err != nil {
		return err
	}
	return nil
}

func processDeleteRoleCollection(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	r := token.Noun

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.DeleteRoleAuth(d, c, r)
}

func processDeleteRoleDatabase(token Token, u auth.User) error {
	d := token.Location
	r := token.Noun

	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.DeleteRoleAuth(d, r)
}

func processDeleteUserCollection(token Token, u auth.User) error {
	parts := strings.Split(token.Location, ".")
	d := parts[0]
	c := parts[1]
	un := token.Noun

	if err := collection.VerifyAuth(d, c, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.DeleteUserAuth(d, c, un)
}

func processDeleteUserDatabase(token Token, u auth.User) error {
	d := token.Location
	un := token.Noun

	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.DeleteRoleAuth(d, un)
}
