package query

import (
	"ceresdb/auth"
	"ceresdb/constants"
	"ceresdb/database"
	"ceresdb/utils"
	"fmt"
)

func CreateDatabase(name string, u auth.User) error {
	if utils.Contains(u.Groups, constants.GROUP_ADMIN) || utils.Contains(u.Roles, constants.ROLE_ADMIN) {
		return database.Create(name)
	}
	return fmt.Errorf("invalid permissions to create database for user %s", u.Username)
}

func DeleteDatabase(name string, u auth.User) error {
	if err := database.VerifyAuth(name, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return database.Delete(name)
}

func GetDatabase(name string, u auth.User) (interface{}, error) {
	if err := database.VerifyAuth(name, constants.PERMISSION_READ, u); err != nil {
		return nil, err
	}
	return database.Databases[name], nil
}

func ListDatabases(u auth.User) interface{} {
	out := []string{}
	for name := range database.Databases {
		if err := database.VerifyAuth(name, constants.PERMISSION_READ, u); err != nil {
			continue
		}
		out = append(out, name)
	}
	return out
}
