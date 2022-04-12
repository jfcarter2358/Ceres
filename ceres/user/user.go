package user

import (
	"ceres/record"
	"errors"
)

func Delete(ids []string) error {
	err := record.Delete("_auth", "_users", ids)
	return err
}

func Get(ids []string) ([]map[string]interface{}, error) {
	data, err := record.Get("_auth", "_users", ids)
	return data, err
}

func Patch() error {
	return errors.New("PATCH action is unsupported on resource USER")
}

func Post(data []map[string]interface{}) error {
	err := record.Post("_auth", "_users", data)
	return err
}

func Put(data []map[string]interface{}) error {
	err := record.Put("_auth", "_users", data)
	return err
}
