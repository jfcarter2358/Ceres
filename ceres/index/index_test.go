package index

import (
	"ceres/config"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestAdd(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	byteData, _ := os.ReadFile(config.Config.CeresDir + "/indices/db1/foo/foo/bar")

	expectedData := string(byteData) + "1234-5678\n"
	var expectedError error
	expectedError = nil

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	err := Add("db1", "foo", inputInterface)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}

	byteData, _ = os.ReadFile(config.Config.CeresDir + "/indices/db1/foo/foo/bar")
	data := string(byteData)

	if data != expectedData {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
}

func TestAddDirNotWritable(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config-not-writable.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = &os.PathError{}

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	err := Add("db1", "foo", inputInterface)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestAddFileNotWritable(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = &os.PathError{}

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo", 0444)

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	err := Add("db1", "foo", inputInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo", 0755)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestDelete(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = nil

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	err := Delete("db1", "foo", inputInterface)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestDeleteStillWithContents(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = nil

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	Add("db1", "foo", inputInterface)

	err := Delete("db1", "foo", inputInterface)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestDeleteReadFileErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = &os.PathError{}

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	Add("db1", "foo", inputInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo", 0444)

	err := Delete("db1", "foo", inputInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo", 0755)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestDeleteErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = &os.PathError{}

	inputInterface := make(map[string]interface{})
	inputData := "{\"foo2\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	json.Unmarshal([]byte(inputData), &inputInterface)

	Add("db1", "foo", inputInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0555)

	err := Delete("db1", "foo", inputInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0755)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestUpdate(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	expectedData := "1234-5678\n"
	var expectedError error
	expectedError = nil

	oldInterface := make(map[string]interface{})
	newInterface := make(map[string]interface{})
	oldData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	newData := "{\"foo\":\"baz\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(oldData), &oldInterface)
	json.Unmarshal([]byte(newData), &newInterface)

	Add("db1", "foo", oldInterface)
	err := Update("db1", "foo", oldInterface, newInterface)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}

	byteData, _ := os.ReadFile(config.Config.CeresDir + "/indices/db1/foo/foo/baz")
	data := string(byteData)

	if data != expectedData {
		t.Errorf("Data was incorrect, got: %v, want: %v", data, expectedData)
	}
}

func TestUpdateErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = &os.PathError{}

	oldInterface := make(map[string]interface{})
	newInterface := make(map[string]interface{})
	oldData := "{\"foo2\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	newData := "{\"foo2\":\"baz\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	json.Unmarshal([]byte(oldData), &oldInterface)
	json.Unmarshal([]byte(newData), &newInterface)

	Add("db1", "foo", oldInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0555)

	err := Update("db1", "foo", oldInterface, newInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0755)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestUpdateErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	var expectedError error
	expectedError = &os.PathError{}

	oldInterface := make(map[string]interface{})
	newInterface := make(map[string]interface{})
	oldData := "{\"foo2\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	newData := "{\"foo2\":\"baz\",\"hello\":1,\"world\":false,\".id\":\"0123-4567\"}"
	json.Unmarshal([]byte(oldData), &oldInterface)
	json.Unmarshal([]byte(newData), &newInterface)

	Add("db1", "foo", oldInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0444)

	err := Update("db1", "foo", oldInterface, newInterface)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0755)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestGet(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	byteData, _ := os.ReadFile(config.Config.CeresDir + "/indices/db1/foo/foo/bar")
	expectedIndices := strings.Split(string(byteData), "\n")
	expectedIndices[len(expectedIndices)-1] = "1234-5678"
	var expectedError error
	expectedError = nil

	oldInterface := make(map[string]interface{})
	oldData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(oldData), &oldInterface)

	Add("db1", "foo", oldInterface)
	stringVal := fmt.Sprintf("%v", "bar")
	indices, err := Get("db1", "foo", "foo", stringVal)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}

	if !reflect.DeepEqual(indices, expectedIndices) {
		t.Errorf("Indices were incorrect, got: %v, want: %v", indices, expectedIndices)
	}
}

func TestGetErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	expectedIndices := []string{}
	expectedIndices = nil
	var expectedError error
	expectedError = &os.PathError{}

	oldInterface := make(map[string]interface{})
	oldData := "{\"foo\":\"bar\",\"hello\":1,\"world\":false,\".id\":\"1234-5678\"}"
	json.Unmarshal([]byte(oldData), &oldInterface)

	Add("db1", "foo", oldInterface)
	stringVal := fmt.Sprintf("%v", "baz")

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0444)

	indices, err := Get("db1", "foo", "foo2", stringVal)

	os.Chmod(config.Config.CeresDir+"/indices/db1/foo/foo2", 0755)

	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}

	if !reflect.DeepEqual(indices, expectedIndices) {
		t.Errorf("Indices were incorrect, got: %v, want: %v", indices, expectedIndices)
	}
}

func TestRemoveIndex(t *testing.T) {
	expectedData := []string{"foo", "bar", "baz"}
	data := removeIndex(expectedData, "foobar")

	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Indices were incorrect, got: %v, want: %v", data, expectedData)
	}
}

func TestAll(t *testing.T) {
	byteData, _ := os.ReadFile(config.Config.CeresDir + "/indices/db1/foo/all")
	expectedData := strings.Split(string(byteData), "\n")
	expectedData = expectedData[:len(expectedData)-1]
	data, _ := All("db1", "foo")
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("Indices were incorrect, got: %v, want: %v", data, expectedData)
	}

}
