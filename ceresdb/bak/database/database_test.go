package database

import (
	"ceresdb/config"
	"ceresdb/freespace"
	"ceresdb/schema"
	"os"
	"reflect"
	"testing"
)

func initialize() {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb/config/config.json")
	config.ReadConfigFile()
	freespace.LoadFreeSpace()
	schema.LoadSchema()
}

func TestDelete(t *testing.T) {
	initialize()
	Post("foo")

	err := Delete("foo")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	Delete("foo")
}

func TestGet(t *testing.T) {
	initialize()
	expectedData := []map[string]interface{}{{"name": "db1"}}
	data, err := Get()
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
}

func TestPost(t *testing.T) {
	initialize()
	err := Post("foo")

	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	expectedData := []map[string]interface{}{{"name": "db1"}, {"name": "foo"}}
	data, err := Get()
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
	Delete("foo")
}

func TestPut(t *testing.T) {
	err := Put()

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}

func TestPatch(t *testing.T) {
	err := Patch()

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
