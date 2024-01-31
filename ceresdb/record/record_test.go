package record

import (
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/database"
	"ceresdb/index"
	"ceresdb/schema"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/uuid"
)

const TEST_DB_NAME = "foo"
const TEST_COLLECTION_NAME = "foo"

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

func TestWrite(t *testing.T) {
	data := map[string]interface{}{
		constants.ID_KEY: "1234.0",
		"a":              "hello",
		"b":              1,
		"c":              3.14,
		"d":              true,
		"e": map[string]interface{}{
			"f": "world",
		},
		"g": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Write(TEST_DB_NAME, TEST_COLLECTION_NAME, data); err != nil {
		t.Errorf(err.Error())
	}
	if err := clean(id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestGetAll(t *testing.T) {
	data := map[string]interface{}{
		"a": "hello",
		"b": 1,
		"c": 3.14,
		"d": true,
		"e": map[string]interface{}{
			"f": "world",
		},
		"g": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Write(TEST_DB_NAME, TEST_COLLECTION_NAME, data); err != nil {
		t.Errorf(err.Error())
	}
	out, err := GetAllIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(out) != 1 {
		t.Errorf("wrong number of records returned, got: %d, want: 1", len(out))
	}
	if err := clean(id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestGet(t *testing.T) {
	data := map[string]interface{}{
		"a": "hello",
		"b": 1,
		"c": 3.14,
		"d": true,
		"e": map[string]interface{}{
			"f": "world",
		},
		"g": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Write(TEST_DB_NAME, TEST_COLLECTION_NAME, data); err != nil {
		t.Errorf(err.Error())
	}
	outAll, err := GetAllIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(outAll) != 1 {
		t.Errorf("wrong number of records returned, got: %d, want: 1", len(outAll))
	}
	outMap := outAll[0].(map[string]interface{})
	mid := outMap[constants.ID_KEY].(string)
	out, err := Get(TEST_DB_NAME, TEST_COLLECTION_NAME, mid)
	if err != nil {
		t.Errorf(err.Error())
	}
	if out == nil {
		t.Errorf("output is nil for id: %s", mid)
	}

	if err := clean(id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestUpdate(t *testing.T) {
	data := map[string]interface{}{
		"a": "hello",
		"b": 1,
		"c": 3.14,
		"d": true,
		"e": map[string]interface{}{
			"f": "world",
		},
		"g": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Write(TEST_DB_NAME, TEST_COLLECTION_NAME, data); err != nil {
		t.Errorf(err.Error())
	}
	outAll, err := GetAllIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(outAll) != 1 {
		t.Errorf("wrong number of records returned, got: %d, want: 1", len(outAll))
	}
	outMap := outAll[0].(map[string]interface{})
	mid := outMap[constants.ID_KEY].(string)
	outMap["b"] = 2
	if err := Update(TEST_DB_NAME, TEST_COLLECTION_NAME, mid, outMap); err != nil {
		t.Errorf(err.Error())
	}
	out, err := Get(TEST_DB_NAME, TEST_COLLECTION_NAME, mid)
	if err != nil {
		t.Errorf(err.Error())
	}
	if out == nil {
		t.Errorf("output is nil for id: %s", mid)
	}
	outMapCheck := out.(map[string]interface{})
	if int(outMapCheck["b"].(float64)) != 2 {
		t.Errorf("record valid mismatch for 'b': got %d, want 2", outMapCheck["b"].(int))
	}
	if err := clean(id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDelete(t *testing.T) {
	data := map[string]interface{}{
		"a": "hello",
		"b": 1,
		"c": 3.14,
		"d": true,
		"e": map[string]interface{}{
			"f": "world",
		},
		"g": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Write(TEST_DB_NAME, TEST_COLLECTION_NAME, data); err != nil {
		t.Errorf(err.Error())
	}
	outAll, err := GetAllIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(outAll) != 1 {
		t.Errorf("wrong number of records returned, got: %d, want: 1", len(outAll))
	}
	outMap := outAll[0].(map[string]interface{})
	mid := outMap[constants.ID_KEY].(string)
	if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME, mid); err != nil {
		t.Errorf(err.Error())
	}
	outAll2, err := GetAllIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(outAll2) != 0 {
		t.Errorf("wrong number of records returned, got: %d, want: 0", len(outAll2))
	}
	if err := clean(id); err != nil {
		t.Errorf(err.Error())
	}
}
