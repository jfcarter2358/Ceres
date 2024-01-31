package index

import (
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/schema"
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

func clean(delete_index bool, id string) error {
	if delete_index {
		if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
			return err
		}
	}
	// if err := database.Delete(TEST_DB_NAME); err != nil {
	// 	return err
	// }
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
	os.MkdirAll(fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id), 0777)
	setupEnv(id)
	if !empty {
		setupFixtures(id)
	}
	config.ReadConfig()
	// if err := database.LoadDatabases(); err != nil {
	// 	return err
	// }
	// if err := database.Create(TEST_DB_NAME); err != nil {
	// 	return err
	// }
	// if err := collection.LoadCollections(); err != nil {
	// 	return err
	// }
	// if err := collection.Create(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
	// 	return err
	// }
	if err := schema.BuildSchema(TEST_DB_NAME, TEST_COLLECTION_NAME, s); err != nil {
		return err
	}
	if err := LoadIndices(); err != nil {
		return err
	}
	if err := LoadIndexKeys(); err != nil {
		return err
	}
	if err := LoadIndexIDs(); err != nil {
		return err
	}
	if err := LoadIndexCache(); err != nil {
		return err
	}
	return nil
}

func TestBuildIndex(t *testing.T) {
	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDelete(t *testing.T) {
	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		t.Errorf("error deleting index: %s", err.Error())
	}
	if _, ok := Indices[TEST_DB_NAME][TEST_COLLECTION_NAME]; ok {
		t.Errorf("delete failed to remove index")
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddToIndex(t *testing.T) {
	input := map[string]interface{}{
		"_id": "abc.0",
		"a":   "foo",
		"b":   0,
		"c":   3.14,
		"d":   true,
		"e": map[string]interface{}{
			"f": "bar",
			"g": map[string]interface{}{
				"h": 1,
			},
		},
		"i": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	AddToIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, input)
	i := Indices[TEST_DB_NAME][TEST_COLLECTION_NAME].(map[string]interface{})
	a := i["a"].(map[string][]string)
	f := a["foo"]
	if f[0] != "abc.0" {
		t.Errorf("index add failed: got: %v, want: abc.0 | %v", f[0], Indices)
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteFromIndex(t *testing.T) {
	input := map[string]interface{}{
		"_id": "abc.0",
		"a":   "foo",
		"b":   0,
		"c":   3.14,
		"d":   true,
		"e": map[string]interface{}{
			"f": "bar",
			"g": map[string]interface{}{
				"h": 1,
			},
		},
		"i": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	AddToIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, input)
	DeleteFromIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, input)
	i := Indices[TEST_DB_NAME][TEST_COLLECTION_NAME].(map[string]interface{})
	a := i["a"].(map[string][]string)
	f := a["foo"]
	if len(f) > 0 {
		t.Errorf("index failed to delete element: got: %v, want: []", f)
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestRetrieveFromIndex(t *testing.T) {
	input := map[string]interface{}{
		"_id": "abc.0",
		"a":   "foo",
		"b":   0,
		"c":   3.14,
		"d":   true,
		"e": map[string]interface{}{
			"f": "bar",
			"g": map[string]interface{}{
				"h": 1,
			},
		},
		"i": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	AddToIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, input)
	m, err := RetrieveFromIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, []string{"e", "f"}, "bar")
	if err != nil {
		t.Errorf("error retrieving ids: %s, index keys: %v", err.Error(), IndexKeys)
	}
	if m[0] != "abc.0" {
		t.Errorf("invalid ids: got: %s, want: abc.0", m)
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestRetrieveValsFromIndex(t *testing.T) {
	input := map[string]interface{}{
		"_id": "abc.0",
		"a":   "foo",
		"b":   0,
		"c":   3.14,
		"d":   true,
		"e": map[string]interface{}{
			"f": "bar",
			"g": map[string]interface{}{
				"h": 1,
			},
		},
		"i": []interface{}{"foobar"},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	BuildIndex(TEST_DB_NAME, TEST_COLLECTION_NAME)
	AddToIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, input)
	d, m, err := RetrieveValsFromIndex(TEST_DB_NAME, TEST_COLLECTION_NAME, []string{"e", "f"})
	if err != nil {
		t.Errorf("error retrieving vals: %s, index keys: %v", err.Error(), IndexKeys)
	}
	if d != constants.DATATYPE_STRING {
		t.Errorf("invalid datatype: got: %s, want: %s", d, constants.DATATYPE_STRING)
	}
	if mm, ok := m.(map[string][]string); !ok {
		t.Errorf("invalid values returned: got %v, want map[string]interface{}", mm)
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}
