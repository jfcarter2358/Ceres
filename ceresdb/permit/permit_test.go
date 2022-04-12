package permit

import (
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/freespace"
	"ceresdb/index"
	"ceresdb/record"
	"ceresdb/schema"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func createDatabase(database string) {
	// Initialize
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-permit/config/config.json")
	config.ReadConfigFile()
	freespace.LoadFreeSpace()
	schema.LoadSchema()

	// Create database
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(indexPath, 0755)
	freespace.FreeSpace.Databases[database] = freespace.FreeSpaceDatabase{}
	schema.Schema.Databases[database] = schema.SchemaDatabase{}
	freespace.WriteFreeSpace()
	schema.WriteSchema()

	// Create auth collection
	collection.Post(database, "_users", map[string]interface{}{"username": "STRING", "role": "STRING"})
	inputData := []map[string]interface{}{{"username": "ceresdb", "role": "ADMIN"}}
	record.Post(database, "_users", inputData)
}

func deleteDatabase(database string) {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	os.RemoveAll(dataPath)
	os.RemoveAll(indexPath)
	delete(freespace.FreeSpace.Databases, database)
	delete(schema.Schema.Databases, database)
}

func TestGet(t *testing.T) {
	createDatabase("foo")

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}}
	ids, _ := index.All("foo", "_users")
	data, err := Get("foo", ids)
	for idx, datum := range data {
		delete(datum, ".id")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	deleteDatabase("foo")
}

func TestPost(t *testing.T) {
	createDatabase("foo")

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}, {"role": "READ", "username": "readonly"}}
	inputData := []map[string]interface{}{{"role": "READ", "username": "readonly"}}
	err := Post("foo", inputData)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	ids, _ := index.All("foo", "_users")
	data, err := Get("foo", ids)
	for idx, datum := range data {
		delete(datum, ".id")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	deleteDatabase("foo")
}

func TestPut(t *testing.T) {
	createDatabase("foo")

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}, {"role": "WRITE", "username": "readonly"}}
	inputData := []map[string]interface{}{{"role": "READ", "username": "readonly"}}
	Post("foo", inputData)
	ids, _ := index.All("foo", "_users")
	id := ids[2]
	inputData = []map[string]interface{}{{".id": id, "role": "WRITE", "username": "readonly"}}
	err := Put("foo", inputData)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	ids, _ = index.All("foo", "_users")
	data, err := Get("foo", ids)
	for idx, datum := range data {
		delete(datum, ".id")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	deleteDatabase("foo")
}

func TestDelete(t *testing.T) {
	createDatabase("foo")

	expectedData := []map[string]interface{}{{"role": "ADMIN", "username": "ceresdb"}}
	inputData := []map[string]interface{}{{"role": "READ", "username": "readonly"}}
	Post("foo", inputData)
	ids, _ := index.All("foo", "_users")
	err := Delete("foo", []string{ids[2]})
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	ids, _ = index.All("foo", "_users")
	data, err := Get("foo", ids)
	for idx, datum := range data {
		delete(datum, ".id")
		data[idx] = datum
	}

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	deleteDatabase("foo")
}

func TestPatch(t *testing.T) {
	err := Patch()

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
