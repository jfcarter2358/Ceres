package auth

import (
	"ceresdb/aql"
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/database"
	"ceresdb/manager"
	"ceresdb/record"
	"ceresdb/utils"
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
		database.Post("_auth")
		collection.Post("_auth", "_users", map[string]interface{}{"username": "STRING", "password": "STRING", "role": "STRING"})
		defaultPassword := os.Getenv("CERESDB_DEFAULT_ADMIN_PASSWORD")
		if defaultPassword == "" {
			defaultPassword = "ceresdb"
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(defaultPassword), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		inputData := []map[string]interface{}{{"username": "ceresdb", "password": string(hash), "role": "ADMIN"}}
		err = record.Post("_auth", "_users", inputData)
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

func VerifyCredentials(username, password string) error {
	nodeL := aql.Node{Value: "username"}
	nodeR := aql.Node{Value: username}
	nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
	getAction := aql.Action{Type: "GET", Resource: "USER", Filter: nodeC}
	data, err := manager.ProcessAction(getAction, []string{}, []map[string]interface{}{}, true)
	if err != nil {
		return err
	}
	if len(data) != 1 {
		return errors.New("user does not exist")
	}
	if !comparePasswords(data[0]["password"].(string), password) {
		return errors.New("invalid password")
	}
	return nil
}

func VerifyUserAction(username, password string, action aql.Action) error {
	dbLevel := []string{"RECORD", "COLLECTION", "PERMIT"}
	nodeL := aql.Node{Value: "username"}
	nodeR := aql.Node{Value: username}
	nodeC := aql.Node{Value: "=", Left: &nodeL, Right: &nodeR}
	getAction := aql.Action{Type: "GET", Resource: "USER", Filter: nodeC}
	data, err := manager.ProcessAction(getAction, []string{}, []map[string]interface{}{}, true)
	if err != nil {
		return err
	}
	if len(data) != 1 {
		return errors.New("user does not exist")
	}
	if !comparePasswords(data[0]["password"].(string), password) {
		return errors.New("invalid password")
	}
	role := data[0]["role"].(string)

	if utils.Contains(dbLevel, action.Resource) {
		parts := strings.Split(action.Identifier, ".")
		database := parts[0]
		getAction := aql.Action{Type: "GET", Resource: "PERMIT", Identifier: database, Filter: nodeC}
		data, err = manager.ProcessAction(getAction, []string{}, []map[string]interface{}{}, false)
		if err != nil {
			return err
		}
		if len(data) != 1 {
			return errors.New("user is not permitted to access this database")
		}
		dbRole := data[0]["role"].(string)
		switch action.Type {
		case "COUNT":
			return nil
		case "DELETE":
			if action.Resource == "PERMIT" || action.Resource == "COLLECTION" {
				if !utils.Contains([]string{"ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			}
			return nil
		case "FILTER":
			return nil
		case "GET":
			return nil
		case "LIMIT":
			return nil
		case "ORDERASC":
			return nil
		case "ORDERDSC":
			return nil
		case "PATCH":
			if action.Resource == "PERMIT" || action.Resource == "COLLECTION" {
				if !utils.Contains([]string{"ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			}
			return nil
		case "POST":
			if action.Resource == "PERMIT" || action.Resource == "COLLECTION" {
				if !utils.Contains([]string{"ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			}
			return nil
		case "PUT":
			if action.Resource == "PERMIT" || action.Resource == "COLLECTION" {
				if !utils.Contains([]string{"ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, dbRole) {
					return errors.New("access denied")
				}
			}
			return nil
		}
	} else {
		switch action.Type {
		case "COUNT":
			return nil
		case "DELETE":
			if action.Resource == "USER" {
				if !utils.Contains([]string{"ADMIN"}, role) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, role) {
					return errors.New("access denied")
				}
			}
			return nil
		case "FILTER":
			return nil
		case "GET":
			return nil
		case "LIMIT":
			return nil
		case "ORDERASC":
			return nil
		case "ORDERDSC":
			return nil
		case "JQ":
			return nil
		case "PATCH":
			if action.Resource == "USER" {
				if !utils.Contains([]string{"ADMIN"}, role) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, role) {
					return errors.New("access denied")
				}
			}
			return nil
		case "POST":
			if action.Resource == "USER" {
				if !utils.Contains([]string{"ADMIN"}, role) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, role) {
					return errors.New("access denied")
				}
			}
			return nil
		case "PUT":
			if action.Resource == "USER" {
				if !utils.Contains([]string{"ADMIN"}, role) {
					return errors.New("access denied")
				}
			} else {
				if !utils.Contains([]string{"WRITE", "ADMIN"}, role) {
					return errors.New("access denied")
				}
			}
			return nil
		}
	}
	return errors.New("invalid action type")
}

func ProtectWrite(action aql.Action) error {
	resources := []string{"RECORD", "COLLECTION"}
	if utils.Contains(resources, action.Resource) {
		parts := strings.Split(action.Identifier, ".")
		db := parts[0]
		if db == "_auth" {
			return errors.New("_auth database is protected from direct manipulation")
		}
		if len(parts) > 1 {
			col := parts[1]
			if col == "_users" {
				return errors.New("_users collection is protected from direct manipulation")
			}
		}
	}
	resources = []string{"DATABASE", "PERMIT"}
	if utils.Contains(resources, action.Resource) {
		if action.Identifier == "_auth" {
			return errors.New("_auth database is protected from direct manipulation")
		}
	}
	return nil
}
