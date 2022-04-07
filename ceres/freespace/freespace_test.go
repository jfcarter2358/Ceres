package freespace

import (
	"ceres/config"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestLoadFreeSpace(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	expectedFreeSpace := FreeSpaceStruct{
		Databases: map[string]FreeSpaceDatabase{
			"db1": {
				Collections: map[string]FreeSpaceCollection{
					"foo": {
						Files: map[string]FreeSpaceFile{
							"bar": {
								Full:   false,
								Blocks: [][]int{{20, 31}},
							},
							"baz": {
								Full:   false,
								Blocks: [][]int{{20, 31}},
							},
						},
					},
					"foo1": {
						Files: map[string]FreeSpaceFile{
							"bar": {
								Full:   false,
								Blocks: [][]int{{20, 31}},
							},
							"baz": {
								Full:   false,
								Blocks: [][]int{{20, 31}},
							},
						},
					},
				},
			},
		},
	}
	var expectedError error
	expectedError = nil

	err := LoadFreeSpace()

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(FreeSpace, expectedFreeSpace) {
		t.Errorf("Free space was incorrect, got: %v, want: %v", FreeSpace, expectedFreeSpace)
	}
}

func TestLoadFreeSpaceNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	config.Config.CeresDir = "../../test/free_space_no_file"

	expectedError := &os.PathError{}

	err := LoadFreeSpace()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestLoadFreeSpaceBadVal(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	config.Config.CeresDir = "../../test/free_space_bad_val"

	expectedError := &json.SyntaxError{}

	err := LoadFreeSpace()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestWriteFreeSpace(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	LoadFreeSpace()

	byteExpectedContents, _ := os.ReadFile("../../test/.ceres/free_space.json")
	expectedContents := string(byteExpectedContents)

	WriteFreeSpace()

	byteContents, _ := os.ReadFile("../../test/.ceres/free_space.json")
	contents := string(byteContents)

	if contents != expectedContents[:len(expectedContents)-1] {
		t.Errorf("Incorrect schema contents, got: %v, want: %v", contents, expectedContents)
	}
}
