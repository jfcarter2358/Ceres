package query

import (
	"ceresdb/auth"
	"ceresdb/constants"
	"ceresdb/index"
	"ceresdb/logger"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
)

func CreateUser(username, password string, u auth.User, bypass bool) error {
	pwd, err := utils.HashAndSalt([]byte(password))
	if err != nil {
		return err
	}
	nu := auth.User{
		Username: username,
		Password: pwd,
		Groups:   []string{},
		Roles:    []string{},
	}
	if bypass {
		nu.Groups = append(nu.Groups, constants.GROUP_ADMIN)
		nu.Roles = append(nu.Roles, constants.ROLE_ADMIN)
	}
	bytes, err := json.Marshal(nu)
	if err != nil {
		return err
	}
	var data interface{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}

	return WriteRecord(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, data, u, bypass)
}

func DeleteUser(username string, u auth.User) error {
	filter := map[string]interface{}{
		"username": username,
	}
	return DeleteRecordIndex(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, filter, u)
}

func GetUser(username string, u auth.User) (auth.User, error) {
	filter := map[string]interface{}{
		"username": username,
	}
	logger.Debugf("", "auth index: %v", index.Indices[constants.AUTH_DB_NAME][constants.AUTH_COLLECTION_NAME])
	data, err := GetRecordIndex(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, filter, u)
	if err != nil {
		return auth.User{}, err
	}
	if len(data) != 1 {
		return auth.User{}, fmt.Errorf("Wrong number of users with username %s, got %d, want 1", username, len(data))
	}
	bytes, err := json.Marshal(data[0])
	if err != nil {
		return auth.User{}, err
	}
	var out auth.User
	if err := json.Unmarshal(bytes, &out); err != nil {
		return auth.User{}, err
	}
	return out, nil
}

func GetUserAll(u auth.User) ([]interface{}, error) {
	out, err := GetRecordAllIndex(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, u)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func UpdateUser(username string, uu auth.User, u auth.User) error {
	filter := map[string]interface{}{
		"username": username,
	}
	bytes, err := json.Marshal(uu)
	if err != nil {
		return err
	}
	var data interface{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}

	return UpdateRecordIndex(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, filter, data, u)
}

func UpdateUserPassword(username, password string, u auth.User) error {
	uu, err := GetUser(username, u)
	if err != nil {
		return err
	}
	pwd, err := utils.HashAndSalt([]byte(password))
	if err != nil {
		return err
	}
	uu.Password = pwd

	return UpdateUser(username, uu, u)
}
