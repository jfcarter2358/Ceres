package aql

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/database"
	"ceresdb/index"
	"ceresdb/schema"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

const TEST_DB_NAME = "foo"
const TEST_COLLECTION_NAME = "foo"

var USER = auth.User{
	Username: "ceresdb",
	Password: "ceresdb",
	Groups:   []string{"admin"},
	Roles:    []string{"admin"},
}

var SCHEMA = map[string]interface{}{
	"a": "string",
	"b": "int",
	"c": "float",
	"d": "bool",
	"e": map[string]interface{}{
		"f": "string",
	},
	"g": []interface{}{"string"},
}

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

func clean(id string) error {
	if err := schema.Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		return err
	}
	if err := index.Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		return err
	}
	if err := collection.Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		return err
	}
	if err := database.Delete(TEST_DB_NAME); err != nil {
		return err
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

func setup(id string) error {
	os.MkdirAll(fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id), 0777)
	setupEnv(id)
	config.ReadConfig()
	if err := database.LoadDatabases(); err != nil {
		return err
	}
	if err := collection.LoadCollections(); err != nil {
		return err
	}
	if err := database.Create(TEST_DB_NAME); err != nil {
		return err
	}
	if err := collection.Create(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		return err
	}
	if err := schema.BuildSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, SCHEMA); err != nil {
		return err
	}
	index.BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	return nil
}

func TestBreakTokens(t *testing.T) {
	inputs := []string{
		"ADD RECORD {\"some\":[\"json\"],\"foo\":1,\"bar\":{\"a\":3.14}} TO foo.bar",
		"GET DATABASE",
	}
	expected := [][]string{
		{
			"ADD",
			"RECORD",
			"{\"some\":[\"json\"],\"foo\":1,\"bar\":{\"a\":3.14}}",
			"TO",
			"foo.bar",
		},
		{
			"GET",
			"DATABASE",
		},
	}
	for idx, input := range inputs {
		out := breakTokens(input)
		for jdx, val := range out {
			if val != expected[idx][jdx] {
				t.Errorf("output did not match expected: got %v, want %v", out, expected)
			}
		}
	}
}
