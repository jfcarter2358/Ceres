package freespace

import (
	"ceresdb/config"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/diff"
)

func TestLoadFreeSpace(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-free_space/config/config.json")
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
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-free_space/config/config.json")
	config.ReadConfigFile()
	config.Config.HomeDir = "../../test/free_space_no_file"

	expectedError := &os.PathError{}

	err := LoadFreeSpace()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestLoadFreeSpaceBadVal(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-free_space/config/config.json")
	config.ReadConfigFile()
	config.Config.HomeDir = "../../test/free_space_bad_val"

	expectedError := &json.SyntaxError{}

	err := LoadFreeSpace()

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestWriteFreeSpace(t *testing.T) {
	os.Setenv("CERESDB_CONFIG_PATH", "../../test/.ceresdb-free_space/config/config.json")
	config.ReadConfigFile()

	LoadFreeSpace()

	byteExpectedContents, _ := os.ReadFile("../../test/.ceresdb-free_space/free_space.json")
	expectedContents := string(byteExpectedContents)

	WriteFreeSpace()

	byteContents, _ := os.ReadFile("../../test/.ceresdb-free_space/free_space.json")
	contents := string(byteContents)

	if a, e := strings.TrimSpace(contents), strings.TrimSpace(expectedContents); a != e {
		t.Errorf("FreeSpace not as expected:\n%v", diff.LineDiff(a, e))
	}
}
