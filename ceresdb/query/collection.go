package query

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/constants"
	"ceresdb/database"
)

func CreateCollection(d, name string, u auth.User) error {
	if err := database.VerifyAuth(d, constants.PERMISSION_WRITE, u); err != nil {
		return err
	}
	return collection.Create(d, name)
}

func DeleteCollection(d, name string, u auth.User) error {
	if err := database.VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	return collection.Delete(d, name)
}

func ListCollections(d string, u auth.User) (interface{}, error) {
	if err := database.VerifyAuth(d, constants.PERMISSION_READ, u); err != nil {
		return nil, err
	}
	return database.Databases[d].Collections, nil
}
