package manager

import (
	"ceres/config"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"strings"
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

func Test_read(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{1, 2}, {4, 4}, {6, 11}}
	var expectedError error
	var expectedData []map[string]interface{}
	expectedError = nil
	for _, block := range blocks {
		for idx := block[0]; idx <= block[1]; idx++ {
			var tempInterface map[string]interface{}
			json.Unmarshal([]byte(fmt.Sprintf("{\"foo\":\"bar\",\".id\":\"bar-%d\"}", idx)), &tempInterface)
			expectedData = append(expectedData, tempInterface)
		}
	}

	actual, err := read("db1", "foo", "bar", blocks)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(actual, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", actual, expectedData)
	}
}

func Test_readNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{1, 2}, {4, 4}, {6, 11}}
	var expectedError error
	var expectedData []map[string]interface{}
	expectedError = &os.PathError{}
	expectedData = nil

	actual, err := read("db1", "foo", "bar12345", blocks)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(actual, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", actual, expectedData)
	}
}

func Test_readBadContents(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{0, 2}, {4, 4}, {6, 11}}
	var expectedError error
	var expectedData []map[string]interface{}
	expectedError = &json.SyntaxError{}
	expectedData = nil

	actual, err := read("db1", "foo", "bad", blocks)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(actual, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", actual, expectedData)
	}
}

func Test_write(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{20, 22}, {24, 25}}
	data := make([]map[string]interface{}, 0)
	expected := "{\"foo\":\"bar\",\".id\":\"bar-0\"}\n{\"foo\":\"bar\",\".id\":\"bar-1\"}\n{\"foo\":\"bar\",\".id\":\"bar-2\"}\n{\"foo\":\"bar\",\".id\":\"bar-3\"}\n{\"foo\":\"bar\",\".id\":\"bar-4\"}\n{\"foo\":\"bar\",\".id\":\"bar-5\"}\n{\"foo\":\"bar\",\".id\":\"bar-6\"}\n{\"foo\":\"bar\",\".id\":\"bar-7\"}\n{\"foo\":\"bar\",\".id\":\"bar-8\"}\n{\"foo\":\"bar\",\".id\":\"bar-9\"}\n{\"foo\":\"bar\",\".id\":\"bar-10\"}\n{\"foo\":\"bar\",\".id\":\"bar-11\"}\n{\"foo\":\"bar\",\".id\":\"bar-12\"}\n{\"foo\":\"bar\",\".id\":\"bar-13\"}\n{\"foo\":\"bar\",\".id\":\"bar-14\"}\n{\"foo\":\"bar\",\".id\":\"bar-15\"}\n{\"foo\":\"bar\",\".id\":\"bar-16\"}\n{\"foo\":\"bar\",\".id\":\"bar-17\"}\n{\"foo\":\"bar\",\".id\":\"bar-18\"}\n{\"foo\":\"bar\",\".id\":\"bar-19\"}\n{\".id\":\"bar-20\",\"foo\":\"bar\"}\n{\".id\":\"bar-21\",\"foo\":\"bar\"}\n{\".id\":\"bar-22\",\"foo\":\"bar\"}\n\n{\".id\":\"bar-24\",\"foo\":\"bar\"}\n{\".id\":\"bar-25\",\"foo\":\"bar\"}\n\n\n\n\n\n\n"
	var expectedError error
	expectedError = nil
	for idx := 20; idx <= 24; idx++ {
		datum := make(map[string]interface{})
		datum["foo"] = "bar"
		data = append(data, datum)
	}

	err := write("db1", "foo", "bar", blocks, data)

	dat, _ := os.ReadFile("../../test/.ceres/data/db1/foo/bar")

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if string(dat) != expected {
		t.Errorf("Data was incorrect, got: %v, want: %v", string(dat), expected)
	}
}

func Test_writeNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{1, 2}, {4, 4}, {6, 11}}
	data := make([]map[string]interface{}, 0)
	var expectedError error
	expectedError = &os.PathError{}

	err := write("db1", "foo", "bar12345", blocks, data)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func Test_writeBadContents(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{20, 22}, {24, 25}}
	data := make([]map[string]interface{}, 0)
	var expectedError error
	expectedError = &json.SyntaxError{}
	for idx := 20; idx <= 24; idx++ {
		datum := make(map[string]interface{})
		datum["foo"] = math.Inf(1)
		data = append(data, datum)
	}

	err := write("db1", "foo", "bar", blocks, data)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func Test_delete(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{20, 22}, {24, 25}}
	expected := "{\"foo\":\"bar\",\".id\":\"bar-0\"}\n{\"foo\":\"bar\",\".id\":\"bar-1\"}\n{\"foo\":\"bar\",\".id\":\"bar-2\"}\n{\"foo\":\"bar\",\".id\":\"bar-3\"}\n{\"foo\":\"bar\",\".id\":\"bar-4\"}\n{\"foo\":\"bar\",\".id\":\"bar-5\"}\n{\"foo\":\"bar\",\".id\":\"bar-6\"}\n{\"foo\":\"bar\",\".id\":\"bar-7\"}\n{\"foo\":\"bar\",\".id\":\"bar-8\"}\n{\"foo\":\"bar\",\".id\":\"bar-9\"}\n{\"foo\":\"bar\",\".id\":\"bar-10\"}\n{\"foo\":\"bar\",\".id\":\"bar-11\"}\n{\"foo\":\"bar\",\".id\":\"bar-12\"}\n{\"foo\":\"bar\",\".id\":\"bar-13\"}\n{\"foo\":\"bar\",\".id\":\"bar-14\"}\n{\"foo\":\"bar\",\".id\":\"bar-15\"}\n{\"foo\":\"bar\",\".id\":\"bar-16\"}\n{\"foo\":\"bar\",\".id\":\"bar-17\"}\n{\"foo\":\"bar\",\".id\":\"bar-18\"}\n{\"foo\":\"bar\",\".id\":\"bar-19\"}\n\n\n\n\n\n\n\n\n\n\n\n\n"
	var expectedError error
	expectedError = nil

	err := delete("db1", "foo", "bar", blocks)

	dat, _ := os.ReadFile("../../test/.ceres/data/db1/foo/bar")

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if string(dat) != expected {
		t.Errorf("Data was incorrect, got: %v, want: %v", string(dat), expected)
	}
}

func Test_deleteNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	blocks := [][]int{{20, 22}, {24, 25}}
	var expectedError error
	expectedError = &os.PathError{}

	err := delete("db1", "foo", "bar12345", blocks)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestRead(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	ids := []string{"bar-1", "bar-2", "bar-4", "bar-6", "bar-7", "bar-8", "bar-9", "bar-10", "bar-11"}
	var expectedError error
	expectedData := make([]map[string]interface{}, 0)
	expectedError = nil
	for _, id := range ids {
		parts := strings.Split(id, "-")
		var tempInterface map[string]interface{}
		json.Unmarshal([]byte(fmt.Sprintf("{\"foo\":\"bar\",\".id\":\"bar-%s\"}", parts[1])), &tempInterface)
		expectedData = append(expectedData, tempInterface)
	}

	actual, err := Read("db1", "foo", ids)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(actual, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", actual, expectedData)
	}
}

func TestReadNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	ids := []string{"bar12345-1", "bar-2", "bar-4", "bar-6", "bar-7", "bar-8", "bar-9", "bar-10", "bar-11"}
	var expectedError error
	expectedData := make([]map[string]interface{}, 0)
	expectedError = &os.PathError{}
	expectedData = nil

	actual, err := Read("db1", "foo", ids)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if !reflect.DeepEqual(actual, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", actual, expectedData)
	}
}

func TestWrite(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	LoadFreeSpace()

	data := make([]map[string]interface{}, 0)
	expected := "{\"foo\":\"bar\",\".id\":\"bar-0\"}\n{\"foo\":\"bar\",\".id\":\"bar-1\"}\n{\"foo\":\"bar\",\".id\":\"bar-2\"}\n{\"foo\":\"bar\",\".id\":\"bar-3\"}\n{\"foo\":\"bar\",\".id\":\"bar-4\"}\n{\"foo\":\"bar\",\".id\":\"bar-5\"}\n{\"foo\":\"bar\",\".id\":\"bar-6\"}\n{\"foo\":\"bar\",\".id\":\"bar-7\"}\n{\"foo\":\"bar\",\".id\":\"bar-8\"}\n{\"foo\":\"bar\",\".id\":\"bar-9\"}\n{\"foo\":\"bar\",\".id\":\"bar-10\"}\n{\"foo\":\"bar\",\".id\":\"bar-11\"}\n{\"foo\":\"bar\",\".id\":\"bar-12\"}\n{\"foo\":\"bar\",\".id\":\"bar-13\"}\n{\"foo\":\"bar\",\".id\":\"bar-14\"}\n{\"foo\":\"bar\",\".id\":\"bar-15\"}\n{\"foo\":\"bar\",\".id\":\"bar-16\"}\n{\"foo\":\"bar\",\".id\":\"bar-17\"}\n{\"foo\":\"bar\",\".id\":\"bar-18\"}\n{\"foo\":\"bar\",\".id\":\"bar-19\"}\n{\".id\":\"bar-20\",\"foo\":\"bar\"}\n{\".id\":\"bar-21\",\"foo\":\"bar\"}\n{\".id\":\"bar-22\",\"foo\":\"bar\"}\n{\".id\":\"bar-23\",\"foo\":\"bar\"}\n{\".id\":\"bar-24\",\"foo\":\"bar\"}\n\n\n\n\n\n\n\n"
	var expectedError error
	expectedError = nil
	for idx := 20; idx <= 24; idx++ {
		datum := make(map[string]interface{})
		datum["foo"] = "bar"
		data = append(data, datum)
	}

	err := Write("db1", "foo", data)

	dat, _ := os.ReadFile("../../test/.ceres/data/db1/foo/bar")

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if string(dat) != expected {
		t.Errorf("Data was incorrect, got: %v, want: %v", string(dat), expected)
	}
}

func TestWriteBadContents(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	LoadFreeSpace()

	data := make([]map[string]interface{}, 0)
	var expectedError error
	expectedError = &json.SyntaxError{}
	for idx := 20; idx <= 24; idx++ {
		datum := make(map[string]interface{})
		datum["foo"] = math.Inf(1)
		data = append(data, datum)
	}

	err := Write("db1", "foo", data)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestWriteOverflow(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	LoadFreeSpace()

	data := make([]map[string]interface{}, 0)
	var expectedError error
	expectedError = nil
	for idx := 0; idx <= 256; idx++ {
		datum := make(map[string]interface{})
		datum["foo"] = "bar"
		data = append(data, datum)
	}

	err := Write("db1", "foo", data)

	files, _ := ioutil.ReadDir("../../test/.ceres/data/db1/foo")

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if len(files) != 14 {
		t.Errorf("Number of files was incorrect, got: %d, want: %d", len(files), 14)
	}
}

func TestDelete(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	LoadFreeSpace()

	ids := []string{"bar-20", "bar-21", "bar-22", "bar-23", "bar-24", "bar-25", "bar-26", "bar-27", "bar-28", "bar-29", "bar-30", "bar-31"}
	var expectedError error
	expected := "{\"foo\":\"bar\",\".id\":\"bar-0\"}\n{\"foo\":\"bar\",\".id\":\"bar-1\"}\n{\"foo\":\"bar\",\".id\":\"bar-2\"}\n{\"foo\":\"bar\",\".id\":\"bar-3\"}\n{\"foo\":\"bar\",\".id\":\"bar-4\"}\n{\"foo\":\"bar\",\".id\":\"bar-5\"}\n{\"foo\":\"bar\",\".id\":\"bar-6\"}\n{\"foo\":\"bar\",\".id\":\"bar-7\"}\n{\"foo\":\"bar\",\".id\":\"bar-8\"}\n{\"foo\":\"bar\",\".id\":\"bar-9\"}\n{\"foo\":\"bar\",\".id\":\"bar-10\"}\n{\"foo\":\"bar\",\".id\":\"bar-11\"}\n{\"foo\":\"bar\",\".id\":\"bar-12\"}\n{\"foo\":\"bar\",\".id\":\"bar-13\"}\n{\"foo\":\"bar\",\".id\":\"bar-14\"}\n{\"foo\":\"bar\",\".id\":\"bar-15\"}\n{\"foo\":\"bar\",\".id\":\"bar-16\"}\n{\"foo\":\"bar\",\".id\":\"bar-17\"}\n{\"foo\":\"bar\",\".id\":\"bar-18\"}\n{\"foo\":\"bar\",\".id\":\"bar-19\"}\n\n\n\n\n\n\n\n\n\n\n\n\n"
	expectedError = nil

	err := Delete("db1", "foo", ids)

	dat, _ := os.ReadFile("../../test/.ceres/data/db1/foo/bar")

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if string(dat) != expected {
		t.Errorf("Data was incorrect, got: %v, want: %v", string(dat), expected)
	}
}

func TestDeleteNoFile(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	ids := []string{"bar12345-20", "bar-21", "bar-22", "bar-23", "bar-24", "bar-25", "bar-26", "bar-27", "bar-28", "bar-29", "bar-30", "bar-31"}
	var expectedError error
	expectedError = &os.PathError{}

	err := Delete("db1", "foo", ids)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}
