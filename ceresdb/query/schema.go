package query

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/constants"
	"ceresdb/schema"
)

func GetSchema(d, name string, u auth.User) (interface{}, error) {
	if err := collection.VerifyAuth(d, name, constants.PERMISSION_READ, u); err != nil {
		return nil, err
	}
	return schema.Schemas[d][name], nil
}

func BuildSchema(d, name string, s interface{}, u auth.User) error {
	if err := collection.VerifyAuth(d, name, constants.PERMISSION_WRITE, u); err != nil {
		return err
	}
	return schema.BuildSchema(d, name, s)
}
