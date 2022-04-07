package schema

import (
	"ceres/config"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestLoadSchema(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
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
		t.Errorf("Schema was incorrect, got: %v, want: %v", Schema, expectedSchema)
	}
}

func TestLoadFreeSpaceNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	config.Config.CeresDir = "../../test/free_space_no_file"

	expectedError := &os.PathError{}

	err := LoadSchema()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestLoadFreeSpaceBadVal(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	config.Config.CeresDir = "../../test/free_space_bad_val"

	expectedError := &json.SyntaxError{}

	err := LoadSchema()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestWriteSchema(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	LoadSchema()

	byteExpectedContents, _ := os.ReadFile("../../test/.ceres/schema.json")
	expectedContents := string(byteExpectedContents)

	WriteSchema()

	byteContents, _ := os.ReadFile("../../test/.ceres/schema.json")
	contents := string(byteContents)

	if contents != expectedContents[:len(expectedContents)-1] {
		t.Errorf("Incorrect schema contents, got: %v, want: %v", contents, expectedContents)
	}
}
