package user

import (
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/database"
	"ceresdb/freespace"
	"ceresdb/index"
	"ceresdb/record"
	"ceresdb/schema"
	"ceresdb/utils"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func initialize() {
	// Initialize
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-user/config/config.json")
	config.ReadConfigFile()
	freespace.LoadFreeSpace()
	schema.LoadSchema()
	os.MkdirAll(config.Config.DataDir, 0755)
	databasePaths, _ := ioutil.ReadDir(config.Config.DataDir)
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
		hash, _ := bcrypt.GenerateFromPassword([]byte(defaultPassword), bcrypt.DefaultCost)
		inputData := []map[string]interface{}{{"username": "ceresdb", "password": string(hash), "role": "ADMIN"}}
		record.Post("_auth", "_users", inputData)
	}
}

func TestGet(t *testing.T) {
	initialize()

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}}
	ids, _ := index.All("_auth", "_users")
	data, err := Get(ids)
	for idx, datum := range data {
		delete(datum, ".id")
		delete(datum, "password")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	database.Delete("_auth")
}

func TestPost(t *testing.T) {
	initialize()

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}, {"role": "READ", "username": "readonly"}}
	inputData := []map[string]interface{}{{"role": "READ", "username": "readonly", "password": "readonly"}}
	err := Post(inputData)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	ids, _ := index.All("_auth", "_users")
	data, err := Get(ids)
	for idx, datum := range data {
		delete(datum, ".id")
		delete(datum, "password")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	database.Delete("_auth")
}

func TestPut(t *testing.T) {
	initialize()

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}, {"role": "WRITE", "username": "readonly"}}
	inputData := []map[string]interface{}{{"role": "READ", "username": "readonly", "password": "readonly"}}
	Post(inputData)
	ids, _ := index.All("_auth", "_users")
	id := ids[2]
	inputData = []map[string]interface{}{{".id": id, "role": "WRITE", "username": "readonly", "password": "readonly"}}
	err := Put(inputData)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	ids, _ = index.All("_auth", "_users")
	data, err := Get(ids)
	for idx, datum := range data {
		delete(datum, ".id")
		delete(datum, "password")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	database.Delete("_auth")
}

func TestDelete(t *testing.T) {
	initialize()

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}}
	inputData := []map[string]interface{}{{"role": "READ", "username": "readonly", "password": "readonly"}}
	Post(inputData)
	ids, _ := index.All("_auth", "_users")
	err := Delete([]string{ids[2]})
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	ids, _ = index.All("_auth", "_users")
	data, err := Get(ids)
	for idx, datum := range data {
		delete(datum, ".id")
		delete(datum, "password")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	database.Delete("_auth")
}

func TestPatch(t *testing.T) {
	err := Patch()

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
