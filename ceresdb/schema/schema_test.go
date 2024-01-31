package schema

import (
	"ceresdb/config"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/uuid"
)

const TEST_DB_NAME = "foo"
const TEST_COLLECTION_NAME = "foo"

func copy(src string, dst string) {
	// Read all content of src to data, may cause OOM for a large file.
	data, err := ioutil.ReadFile(src)
	if err != nil {
		panic(err)
	}
	// Write data to dst
	if err := ioutil.WriteFile(dst, data, 0644); err != nil {
		panic(err)
	}
}

func cleanEnv() {
	os.Setenv("CERESDB_STORAGE_LINE_LIMIT", "")
	os.Setenv("CERESDB_DATA_DIR", "")
}

func cleanFixtures(id string) {
	os.RemoveAll(fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id))
}

func clean(delete_schema bool, id string) error {
	if delete_schema {
		if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
			return err
		}
	}
	cleanFixtures(id)
	cleanEnv()
	return nil
}

func setupEnv(id string) {
	os.Setenv("CERESDB_STORAGE_LINE_LIMIT", "10")
	os.Setenv("CERESDB_DATA_DIR", fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id))
}

func setupFixtures(id string) {
	copy("../../test/fixtures/databases.json", fmt.Sprintf("/tmp/ceresdb/fixtures/%s/databases.json", id))
	copy("../../test/fixtures/collections.json", fmt.Sprintf("/tmp/ceresdb/fixtures/%s/collections.json", id))
}

func setup(empty bool, id string) error {
	os.MkdirAll(fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id), 0777)
	setupEnv(id)
	if !empty {
		setupFixtures(id)
	}
	config.ReadConfig()
	if err := LoadSchemas(); err != nil {
		return err
	}
	return nil
}

func TestBuildSchema(t *testing.T) {
	inputGood := map[string]interface{}{
		"a": "string",
		"b": "int",
		"c": "float",
		"d": "bool",
		"e": map[string]interface{}{
			"f": "string",
			"g": map[string]interface{}{
				"h": "int",
			},
		},
		"i": []interface{}{"string"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}

	if err := BuildSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, inputGood); err != nil {
		t.Errorf(err.Error())
	}

	if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		t.Errorf("error deleting schema: %s", err.Error())
	}

	inputBad := map[string]interface{}{
		"a": "foobar",
		"b": "int",
		"c": "float",
		"d": "bool",
		"e": map[string]interface{}{
			"f": "string",
			"g": map[string]interface{}{
				"h": "int",
			},
		},
		"i": []interface{}{"string"},
	}

	if err := BuildSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, inputBad); err == nil {
		t.Errorf("expected error on schema build, got nil")
	}

	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestValidateSchema(t *testing.T) {
	s := map[string]interface{}{
		"a": "string",
		"b": "int",
		"c": "float",
		"d": "bool",
		"e": map[string]interface{}{
			"f": "string",
			"g": map[string]interface{}{
				"h": "int",
			},
		},
		"i": []interface{}{"string"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}

	if err := BuildSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, s); err != nil {
		t.Errorf(err.Error())
	}

	inputGood := map[string]interface{}{
		"a": "foo",
		"b": 0,
		"c": 3.14,
		"d": true,
		"e": map[string]interface{}{
			"f": "bar",
			"g": map[string]interface{}{
				"h": 1,
			},
		},
		"i": []interface{}{"baz"},
	}

	if err := ValidateSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, inputGood); err != nil {
		t.Errorf(err.Error())
	}

	inputBad := map[string]interface{}{
		"a": "foo",
		"b": 0,
		"c": "hello world",
		"d": true,
		"e": map[string]interface{}{
			"f": "bar",
			"g": map[string]interface{}{
				"h": 1,
			},
		},
		"i": []interface{}{"baz"},
	}

	if err := ValidateSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, inputBad); err == nil {
		t.Errorf("expected error on schema validate, got nil")
	}

	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDelete(t *testing.T) {
	input := map[string]interface{}{
		"a": "string",
		"b": "int",
		"c": "float",
		"d": "bool",
		"e": map[string]interface{}{
			"f": "string",
			"g": map[string]interface{}{
				"h": "int",
			},
		},
		"i": []interface{}{"string"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}

	if err := BuildSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, input); err != nil {
		t.Errorf(err.Error())
	}

	if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		t.Errorf("error deleting schema: %s", err.Error())
	}

	if err := clean(false, id); err != nil {
		t.Errorf(err.Error())
	}
}
