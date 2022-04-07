package auth

import (
	"ceres/aql"
	"ceres/config"
	"ceres/manager"
	"ceres/utils"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func CheckAuthDatabase() error {
	databasePaths, err := ioutil.ReadDir(config.Config.DataDir)
	if err != nil {
		return err
	}
	databases := make([]string, len(databasePaths))
	for idx, db := range databasePaths {
		databases[idx] = db.Name()
	}
	if !utils.Contains(databases, "_auth") {
		manager.CreateDatabase("_auth")
		manager.CreateCollection("_auth", "_users", map[string]interface{}{"username": "STRING", "password": "STRING", "role": "STRING"})
		defaultPassword := os.Getenv("CERES_DEFAULT_ADMIN_PASSWORD")
		if defaultPassword == "" {
			defaultPassword = "ceres"
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(defaultPassword), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		inputData := []map[string]interface{}{{"username": "ceres", "password": string(hash), "role": "ADMIN"}}
		action := aql.Action{Type: "POST", Identifier: "_auth._users", Data: inputData}
		_, err = manager.ProcessAction(action, []string{})
		if err != nil {
			return err
		}
	}
	return nil
}

func comparePasswords(hashedPassword string, plainPassword string) bool {
	// Since we'll be getting the hashed password from the DB it
	// will be a string so we'll need to convert it to a byte slice
	byteHash := []byte(hashedPassword)
	bytePlain := []byte(plainPassword)
	err := bcrypt.CompareHashAndPassword(byteHash, bytePlain)
	if err != nil {
		log.Println(err)
		return false
	}

	return true
}

func VerifyUserAction(username, password string, action aql.Action) error {
	dbLevel := []string{"GET", "COUNT", "POST", "PATCH", "PUT", "DELETE", "FILTER", "LIMIT", "ORDERASC", "ORDERDSC", "DBDEL", "COLADD", "COLDEL", "COLMOD", "PERMITADD", "PERMITDEL", "PERMITMOD", "PERMITGET"}
	nodeL := aql.Node{Value: "username"}
	nodeR := aql.Node{Value: username}
	nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
	getAction := aql.Action{Type: "GET", Identifier: "_auth._users", Filter: nodeC}
	data, err := manager.ProcessAction(getAction, []string{})
	if err != nil {
		return err
	}
	if len(data) != 1 {
		return errors.New("User does not exist")
	}
	if !comparePasswords(data[0]["password"].(string), password) {
		return errors.New("Invalid password")
	}
	role := data[0]["role"].(string)

	if utils.Contains(dbLevel, action.Type) {
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		getAction := aql.Action{Type: "GET", Identifier: database + "._users", Filter: nodeC}
		data, err = manager.ProcessAction(getAction, []string{})
		if err != nil {
			return err
		}
		if len(data) != 1 {
			return errors.New("User is not permitted to access this database")
		}
		dbRole := data[0]["role"].(string)
		switch action.Type {
		case "GET":
			return nil
		case "COUNT":
			return nil
		case "POST":
			if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "PATCH":
			if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "PUT":
			if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "DELETE":
			if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "FILTER":
			return nil
		case "LIMIT":
			return nil
		case "ORDERASC":
			return nil
		case "ORDERDSC":
			return nil
		case "DBDEL":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "COLADD":
			if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "COLDEL":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "COLMOD":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "PERMITADD":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "PERMITDEL":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "PERMITMOD":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		case "PERMITGET":
			if !utils.Contains([]string{"ADMIN"}, dbRole) {
				return errors.New("Access denied")
			}
			return nil
		}
	} else {
		switch action.Type {
		case "DBADD":
			if !utils.Contains([]string{"WRITE", "ADMIN"}, role) {
				return errors.New("Access denied")
			}
			return nil
		case "USERADD":
			if !utils.Contains([]string{"ADMIN"}, role) {
				return errors.New("Access denied")
			}
			return nil
		case "USERDEL":
			if !utils.Contains([]string{"ADMIN"}, role) {
				return errors.New("Access denied")
			}
			return nil
		case "USERMOD":
			if !utils.Contains([]string{"ADMIN"}, role) {
				return errors.New("Access denied")
			}
			return nil
		case "USERGET":
			if !utils.Contains([]string{"ADMIN"}, role) {
				return errors.New("Access denied")
			}
			return nil
		}
	}
	return errors.New("Invalid action type")
}

func ProtectWrite(action aql.Action) error {
	if (action.Type == "DBDEL" || action.Type == "DBADD") && action.Identifier == "_auth" {
		return errors.New("_auth database is protected from direct manipulation")
	}
	actions := []string{"COLDEL", "GET", "POST", "PATCH", "PUT", "DELETE"}
	if utils.Contains(actions, action.Type) {
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		collection := parts[1]
		if database == "_auth" {
			return errors.New("_auth database is protected from direct manipulation")
		}
		if collection == "_users" {
			return errors.New("_users collection is protected from direct manipulation")
		}
	}
	return nil
}
