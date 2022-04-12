package collection

import (
	"ceres/config"
	"ceres/freespace"
	"ceres/schema"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func createDatabase(database string) {
	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	freespace.LoadFreeSpace()
	schema.LoadSchema()
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(indexPath, 0755)
	freespace.FreeSpace.Databases[database] = freespace.FreeSpaceDatabase{}
	schema.Schema.Databases[database] = schema.SchemaDatabase{}
	freespace.WriteFreeSpace()
	schema.WriteSchema()
}

func deleteDatabase(database string) {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	os.RemoveAll(dataPath)
	os.RemoveAll(indexPath)
	delete(freespace.FreeSpace.Databases, database)
	delete(schema.Schema.Databases, database)
}
func TestDelete(t *testing.T) {
	createDatabase("foo")
	Post("foo", "bar", map[string]interface{}{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "LIST"})

	err := Delete("foo", "bar")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	deleteDatabase("foo")
}

func TestGet(t *testing.T) {
	createDatabase("foo")
	Post("foo", "bar", map[string]interface{}{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "LIST"})

	expectedData := []map[string]interface{}{{"name": "bar", "schema": map[string]string{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "LIST"}}}
	data, err := Get("foo")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	deleteDatabase("foo")
}

func TestPost(t *testing.T) {
	createDatabase("foo")

	err := Post("foo", "bar", map[string]interface{}{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "LIST"})

	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	expectedData := []map[string]interface{}{{"name": "bar", "schema": map[string]string{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "LIST"}}}
	data, err := Get("foo")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	deleteDatabase("foo")
}

func TestPut(t *testing.T) {
	createDatabase("foo")

	Post("foo", "bar", map[string]interface{}{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "LIST"})
	err := Put("foo", "bar", map[string]interface{}{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "STRING"})

	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	expectedData := []map[string]interface{}{{"name": "bar", "schema": map[string]string{"a": "STRING", "b": "INT", "c": "FLOAT", "d": "BOOL", "e": "DICT", "f": "STRING"}}}
	data, err := Get("foo")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	deleteDatabase("foo")
}

func TestPatch(t *testing.T) {
	err := Patch()

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
