package permit

import (
	"ceres/record"
	"errors"
)

func Delete(database string, ids []string) error {
	err := record.Delete(database, "_users", ids)
	return err
}

func Get(database string, ids []string) ([]map[string]interface{}, error) {
	data, err := record.Get(database, "_users", ids)
	return data, err
}

func Patch() error {
	return errors.New("PATCH action is unsupported on resource PERMIT")
}

func Post(database string, data []map[string]interface{}) error {
	err := record.Post(database, "_users", data)
	return err
}

func Put(database string, data []map[string]interface{}) error {
	err := record.Put(database, "_users", data)
	return err
}
