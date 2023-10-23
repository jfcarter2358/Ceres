package schema

import (
	"ceresdb/config"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/diff"
)

func TestLoadSchema(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-schema/config/config.json")
	config.ReadConfigFile()

	expectedSchema := SchemaStruct{
		Databases: map[string]SchemaDatabase{
			"db1": {
				Collections: map[string]SchemaCollection{
					"foo": {
						Types: map[string]string{
							"foo": "STRING",
						},
					},
					"foo1": {
						Types: map[string]string{
							"foo": "STRING",
						},
					},
				},
			},
		},
	}
	var expectedError error
	expectedError = nil

	err := LoadSchema()

	if err != expectedError {
		t.Errorf("Schema incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(Schema.Databases["db1"], expectedSchema.Databases["db1"]) {
		t.Errorf("Schema not as expected:\n%v", diff.LineDiff(fmt.Sprintf("%v", Schema), fmt.Sprintf("%v", expectedSchema)))
	}
}

func TestSchemaNoFile(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-schema/config/config.json")
	config.ReadConfigFile()
	config.Config.HomeDir = "../../test/free_space_no_file"

	expectedError := &os.PathError{}

	err := LoadSchema()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestSchemaBadVal(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-schema/config/config.json")
	config.ReadConfigFile()
	config.Config.HomeDir = "../../test/free_space_bad_val"

	expectedError := &json.SyntaxError{}

	err := LoadSchema()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestWriteSchema(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-schema/config/config.json")
	config.ReadConfigFile()

	LoadSchema()

	byteExpectedContents, _ := os.ReadFile("../../test/.ceresdb-schema/schema.json")
	expectedContents := string(byteExpectedContents)

	WriteSchema()

	byteContents, _ := os.ReadFile("../../test/.ceresdb-schema/schema.json")
	contents := string(byteContents)

	if a, e := strings.TrimSpace(contents), strings.TrimSpace(expectedContents); a != e {
		t.Errorf("Schema not as expected:\n%v", diff.LineDiff(a, e))
	}
}

func TestValidateSchemaCollection(t *testing.T) {
	newSchema := map[string]string{"a": "INT", "b": "BOOL", "c": "FLOAT", "d": "STRING", "e": "DICT", "f": "LIST"}
	err := ValidateSchemaCollection(newSchema)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}
	newSchema = map[string]string{"a": "INT", "b": "BOOL", "c": "FLOAT", "d": "STRING", "e": "DICT", "f": "FOOBAR"}
	err = ValidateSchemaCollection(newSchema)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}

func TestValidateDataAgainstSchema(t *testing.T) {
	Schema.Databases["foobar"] = SchemaDatabase{Collections: map[string]SchemaCollection{"baz": {Types: map[string]string{"a": "INT", "b": "BOOL", "c": "FLOAT", "d": "STRING", "e": "DICT", "f": "LIST"}}}}

	inputData := []map[string]interface{}{{".id": "abc", "a": 0, "b": true, "c": 1.0, "d": "string", "e": map[string]interface{}{"hello": "world"}, "f": []interface{}{"foo", "bar"}}}
	err := ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	inputData = []map[string]interface{}{{"a": "foo", "b": true, "c": 1.0, "d": "string", "e": map[string]interface{}{"hello": "world"}, "f": []interface{}{"foo", "bar"}}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	inputData = []map[string]interface{}{{"a": 0, "b": "foo", "c": 1.0, "d": "string", "e": map[string]interface{}{"hello": "world"}, "f": []interface{}{"foo", "bar"}}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	inputData = []map[string]interface{}{{"a": 0, "b": true, "c": "foo", "d": "string", "e": map[string]interface{}{"hello": "world"}, "f": []interface{}{"foo", "bar"}}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	inputData = []map[string]interface{}{{"a": 0, "b": true, "c": 1.0, "d": 0, "e": map[string]interface{}{"hello": "world"}, "f": []interface{}{"foo", "bar"}}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	inputData = []map[string]interface{}{{"a": 0, "b": true, "c": 1.0, "d": "string", "e": "foo", "f": []interface{}{"foo", "bar"}}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	inputData = []map[string]interface{}{{"a": 0, "b": true, "c": 1.0, "d": "string", "e": map[string]interface{}{"hello": "world"}, "f": "foo"}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	inputData = []map[string]interface{}{{"hello": "world", "a": 0, "b": true, "c": 1.0, "d": "string", "e": map[string]interface{}{"hello": "world"}, "f": []interface{}{"foo", "bar"}}}
	err = ValidateDataAgainstSchema("foobar", "baz", inputData)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
