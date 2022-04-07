package aql

import (
	"ceres/config"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestDetermineType(t *testing.T) {
	var value string
	var tok Token
	value = ","
	determineType(value, &tok)
	if tok.Type != "COMMA" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "COMMA")
	}
	value = "("
	determineType(value, &tok)
	if tok.Type != "OPEN_PAREN" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OPEN_PAREN")
	}
	value = ")"
	determineType(value, &tok)
	if tok.Type != "CLOSE_PAREN" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "CLOSE_PAREN")
	}
	value = "["
	determineType(value, &tok)
	if tok.Type != "OPEN_BRACKET" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OPEN_BRACKET")
	}
	value = "]"
	determineType(value, &tok)
	if tok.Type != "CLOSE_BRACKET" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "CLOSE_BRACKET")
	}
	value = "{"
	determineType(value, &tok)
	if tok.Type != "OPEN_BRACE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OPEN_BRACE")
	}
	value = "}"
	determineType(value, &tok)
	if tok.Type != "CLOSE_BRACE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "CLOSE_BRACE")
	}
	value = "|"
	determineType(value, &tok)
	if tok.Type != "PIPE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "PIPE")
	}
	value = "*"
	determineType(value, &tok)
	if tok.Type != "WILDCARD" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "WILDCARD")
	}
	value = "GET"
	determineType(value, &tok)
	if tok.Type != "GET" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "GET")
	}
	value = "POST"
	determineType(value, &tok)
	if tok.Type != "POST" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "POST")
	}
	value = "PUT"
	determineType(value, &tok)
	if tok.Type != "PUT" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "PUT")
	}
	value = "PATCH"
	determineType(value, &tok)
	if tok.Type != "PATCH" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "PATCH")
	}
	value = "DELETE"
	determineType(value, &tok)
	if tok.Type != "DELETE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "DELETE")
	}
	value = "DBADD"
	determineType(value, &tok)
	if tok.Type != "DBADD" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "DBADD")
	}
	value = "COLADD"
	determineType(value, &tok)
	if tok.Type != "COLADD" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "COLADD")
	}
	value = "DBDEL"
	determineType(value, &tok)
	if tok.Type != "DBDEL" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "DBDEL")
	}
	value = "COLDEL"
	determineType(value, &tok)
	if tok.Type != "COLDEL" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "COLDEL")
	}
	value = "LIMIT"
	determineType(value, &tok)
	if tok.Type != "LIMIT" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "LIMIT")
	}
	value = "FILTER"
	determineType(value, &tok)
	if tok.Type != "FILTER" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "FILTER")
	}
	value = "ORDERASC"
	determineType(value, &tok)
	if tok.Type != "ORDERASC" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "ORDERASC")
	}
	value = "ORDERDSC"
	determineType(value, &tok)
	if tok.Type != "ORDERDSC" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "ORDERDSC")
	}
	value = "-"
	determineType(value, &tok)
	if tok.Type != "DASH" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "DASH")
	}
	value = "AND"
	determineType(value, &tok)
	if tok.Type != "LOGIC" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "LOGIC")
	}
	value = "OR"
	determineType(value, &tok)
	if tok.Type != "LOGIC" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "LOGIC")
	}
	value = "XOR"
	determineType(value, &tok)
	if tok.Type != "LOGIC" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "LOGIC")
	}
	value = "NOT"
	determineType(value, &tok)
	if tok.Type != "LOGIC" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "LOGIC")
	}
	value = ">"
	determineType(value, &tok)
	if tok.Type != "OP" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OP")
	}
	value = ">="
	determineType(value, &tok)
	if tok.Type != "OP" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OP")
	}
	value = "="
	determineType(value, &tok)
	if tok.Type != "OP" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OP")
	}
	value = "<="
	determineType(value, &tok)
	if tok.Type != "OP" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OP")
	}
	value = "<"
	determineType(value, &tok)
	if tok.Type != "OP" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OP")
	}
	value = "!="
	determineType(value, &tok)
	if tok.Type != "OP" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "OP")
	}
	value = "\"string\""
	determineType(value, &tok)
	if tok.Type != "STRING" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "STRING")
	}
	value = "\"string\""
	determineType(value, &tok)
	if tok.Type != "STRING" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "STRING")
	}
	value = "[list]"
	determineType(value, &tok)
	if tok.Type != "LIST" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "LIST")
	}
	value = "{dict}"
	determineType(value, &tok)
	if tok.Type != "DICT" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "DICT")
	}
	value = "1234"
	determineType(value, &tok)
	if tok.Type != "INT" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "INT")
	}
	value = "12.34"
	determineType(value, &tok)
	if tok.Type != "FLOAT" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "FLOAT")
	}
	value = "true"
	determineType(value, &tok)
	if tok.Type != "BOOLEAN" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "BOOLEAN")
	}
	value = "false"
	determineType(value, &tok)
	if tok.Type != "BOOLEAN" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "BOOLEAN")
	}
	value = "database.collection"
	determineType(value, &tok)
	if tok.Type != "IDENTIFIER" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "IDENTIFIER")
	}
	value = "index"
	determineType(value, &tok)
	if tok.Type != "FIELD" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "FIELD")
	}
}

func TestGetPatterns(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	expectedData := "GET IDENTIFIER(?: WILDCARD|LIST|STRING)?"
	var expectedError error
	expectedError = nil

	patterns, err := getPatterns()

	if patterns["GET"] != expectedData {
		t.Errorf("Pattern was incorrect, got: %v, want: %v", patterns["GET"], expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want %v", err, expectedError)
	}
}

func TestGetPatternsErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config-no-aql.json")
	config.ReadConfigFile()

	var expectedData map[string]string
	expectedData = nil
	var expectedError error
	expectedError = &os.PathError{}

	patterns, err := getPatterns()

	if !reflect.DeepEqual(patterns, expectedData) {
		t.Errorf("Pattern was incorrect, got: %v, want: %v", patterns, expectedData)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want %v", err, expectedError)
	}
}

func TestGetPatternsErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres-bad-aql/config/config.json")
	config.ReadConfigFile()

	var expectedData map[string]string
	expectedData = nil
	var expectedError error
	expectedError = &json.SyntaxError{}

	patterns, err := getPatterns()

	if !reflect.DeepEqual(patterns, expectedData) {
		t.Errorf("Pattern was incorrect, got: %v, want: %v", patterns, expectedData)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestCheckPattern(t *testing.T) {
	err := checkPattern("foo bar", "[f]oo bar", "[f]oo bar")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, nil)
	}
}

func TestCheckPatternErr(t *testing.T) {
	err := checkPattern("boo bar", "[f]oo bar", "[f]oo bar")
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, nil)
	}
}

func TestHandleConditionals(t *testing.T) {
	tokens := []Token{{Type: "LOGIC", Value: "AND"}, {Type: "STRING", Value: "foobar"}, {Type: "STRING", Value: "foobar"}}
	node := handleConditionals(tokens)

	if node.Value != "AND" {
		t.Errorf("Incorrect node value, got: %v, want: %v", node.Value, "AND")
	}
}

func TestHandleDataList(t *testing.T) {
	var action Action
	expectedData := []map[string]interface{}{{"foo": "bar"}}
	var expectedError error
	expectedError = nil

	token := Token{Type: "LIST", Value: "[{\"foo\":\"bar\"}]"}
	err := handleData(token, &action)

	if !reflect.DeepEqual(action.Data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Data, expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleDataListErr(t *testing.T) {
	var action Action
	var expectedData []map[string]interface{}
	expectedData = nil
	var expectedError error
	expectedError = &json.SyntaxError{}

	token := Token{Type: "LIST", Value: "[{\"foo\":\"bar\"}"}
	err := handleData(token, &action)

	if !reflect.DeepEqual(action.Data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Data, expectedData)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleDataDict(t *testing.T) {
	var action Action
	expectedData := []map[string]interface{}{{"foo": "bar"}}
	var expectedError error
	expectedError = nil

	token := Token{Type: "DICT", Value: "{\"foo\":\"bar\"}"}
	err := handleData(token, &action)

	if !reflect.DeepEqual(action.Data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Data, expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleDataDictErr(t *testing.T) {
	var action Action
	var expectedData []map[string]interface{}
	expectedData = nil
	var expectedError error
	expectedError = &json.SyntaxError{}

	token := Token{Type: "DICT", Value: "{\"foo\":\"bar\""}
	err := handleData(token, &action)

	if !reflect.DeepEqual(action.Data, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Data, expectedData)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleIdsList(t *testing.T) {
	var action Action
	expectedData := []string{"foo", "bar"}
	var expectedError error
	expectedError = nil

	token := Token{Type: "LIST", Value: "[\"foo\",\"bar\"]"}
	err := handleIDs(token, &action)

	if !reflect.DeepEqual(action.IDs, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.IDs, expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleIDsListErr(t *testing.T) {
	var action Action
	var expectedData []string
	expectedData = nil
	var expectedError error
	expectedError = &json.SyntaxError{}

	token := Token{Type: "LIST", Value: "[\"foo\",\"bar\""}
	err := handleIDs(token, &action)

	if !reflect.DeepEqual(action.IDs, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.IDs, expectedData)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleIDsString(t *testing.T) {
	var action Action
	expectedData := []string{"foo"}
	var expectedError error
	expectedError = nil

	token := Token{Type: "STRING", Value: "foo"}
	err := handleIDs(token, &action)

	if !reflect.DeepEqual(action.IDs, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.IDs, expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleFieldsList(t *testing.T) {
	var action Action
	expectedData := []string{"foo", "bar"}
	var expectedError error
	expectedError = nil

	token := Token{Type: "LIST", Value: "[\"foo\",\"bar\"]"}
	err := handleFields(token, &action)

	if !reflect.DeepEqual(action.Fields, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Fields, expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleFieldsListErr(t *testing.T) {
	var action Action
	var expectedData []string
	expectedData = nil
	var expectedError error
	expectedError = &json.SyntaxError{}

	token := Token{Type: "LIST", Value: "[\"foo\",\"bar\""}
	err := handleFields(token, &action)

	if !reflect.DeepEqual(action.Fields, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Fields, expectedData)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestHandleFieldsString(t *testing.T) {
	var action Action
	expectedData := []string{"foo"}
	var expectedError error
	expectedError = nil

	token := Token{Type: "STRING", Value: "foo"}
	err := handleFields(token, &action)

	if !reflect.DeepEqual(action.Fields, expectedData) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action.Fields, expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsGet(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "GET", Value: "GET"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "WILDCARD", Value: "*"}, {Type: "PIPE", Value: "|"}, {Type: "GET", Value: "GET"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "WILDCARD", Value: "*"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsGetErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "GET", Value: "GET"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsGetErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "GET", Value: "GET"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "LIST", Value: "[\"foo\",\"bar\""}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPost(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "POST", Value: "POST"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DICT", Value: "{\"foo\":\"bar\"}"}, {Type: "PIPE", Value: "|"}, {Type: "POST", Value: "POST"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DICT", Value: "{\"foo\":\"bar\"}"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsPostErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "POST", Value: "POST"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPostErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "POST", Value: "POST"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "LIST", Value: "[{\"foo\":\"bar\""}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPut(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "PUT", Value: "PUT"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DICT", Value: "{\"foo\":\"bar\"}"}, {Type: "PIPE", Value: "|"}, {Type: "PUT", Value: "PUT"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DICT", Value: "{\"foo\":\"bar\"}"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsPutErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "PUT", Value: "PUT"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPutErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "PUT", Value: "PUT"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "LIST", Value: "[{\"foo\":\"bar\""}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPatch(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "PATCH", Value: "PATCH"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DASH", Value: "-"}, {Type: "DICT", Value: "{\"foo\":\"bar\"}"}, {Type: "PIPE", Value: "|"}, {Type: "PATCH", Value: "PATCH"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DASH", Value: "-"}, {Type: "DICT", Value: "{\"foo\":\"bar\"}"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsPatchErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "PATCH", Value: "PATCH"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPatchErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "PATCH", Value: "PATCH"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "LIST", Value: "[\"foo\",\"bar\""}, {Type: "LIST", Value: "[{\"foo\":\"bar\"}]"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsPatchErr3(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "PATCH", Value: "PATCH"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "LIST", Value: "[\"foo\",\"bar\"]"}, {Type: "LIST", Value: "[{\"foo\":\"bar\""}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsDelete(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "DELETE", Value: "DELETE"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DASH", Value: "-"}, {Type: "PIPE", Value: "|"}, {Type: "DELETE", Value: "DELETE"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "DASH", Value: "-"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsDeleteErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "DELETE", Value: "DELETE"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsDeleteErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "DELETE", Value: "DELETE"}, {Type: "IDENTIFIER", Value: "db.col"}, {Type: "LIST", Value: "[{\"foo\":\"bar\""}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsFilter(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "FILTER", Value: "FILTER"}, {Type: "FIELD", Value: "level"}, {Type: "OP", Value: "="}, {Type: "STRING", Value: "debug"}, {Type: "PIPE", Value: "|"}, {Type: "FILTER", Value: "FILTER"}, {Type: "FIELD", Value: "level"}, {Type: "OP", Value: "="}, {Type: "STRING", Value: "debug"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsFilterErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "FILTER", Value: "FILTER"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsLimit(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "LIMIT", Value: "LIMIT"}, {Type: "INT", Value: "10"}, {Type: "PIPE", Value: "|"}, {Type: "LIMIT", Value: "LIMIT"}, {Type: "INT", Value: "10"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsLimitErr1(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "LIMIT", Value: "LIMIT"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsLimitErr2(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "LIMIT", Value: "LIMIT"}, {Type: "INT", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsOrderASC(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "ORDERASC", Value: "ORDERASC"}, {Type: "FIELD", Value: "level"}, {Type: "PIPE", Value: "|"}, {Type: "ORDERASC", Value: "ORDERASC"}, {Type: "FIELD", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsOrderASCErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "ORDERASC", Value: "ORDERASC"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsOrderDSC(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "ORDERDSC", Value: "ORDERDSC"}, {Type: "FIELD", Value: "level"}, {Type: "PIPE", Value: "|"}, {Type: "ORDERDSC", Value: "ORDERDSC"}, {Type: "FIELD", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsOrderDSCErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "ORDERDSC", Value: "ORDERDSC"}, {Type: "STRING", Value: "level"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsCreateDB(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "DBADD", Value: "DBADD"}, {Type: "FIELD", Value: "db"}, {Type: "PIPE", Value: "|"}, {Type: "DBADD", Value: "DBADD"}, {Type: "FIELD", Value: "db"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsCreateDBErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "DBADD", Value: "DBADD"}, {Type: "INT", Value: "42"}, {Type: "PIPE", Value: "|"}, {Type: "DBADD", Value: "DBADD"}, {Type: "FIELD", Value: "db"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsCreateCOL(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "COLADD", Value: "COLADD"}, {Type: "IDENTIFIER", Value: "db.foo"}, {Type: "DICT", Value: "{\"foo\":\"STRING\"}"}, {Type: "PIPE", Value: "|"}, {Type: "COLADD", Value: "COLADD"}, {Type: "IDENTIFIER", Value: "db.foo"}, {Type: "DICT", Value: "{\"foo\":\"STRING\"}"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsCreateCOLErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "COLADD", Value: "COLADD"}, {Type: "INT", Value: "42"}, {Type: "PIPE", Value: "|"}, {Type: "COLADD", Value: "COLADD"}, {Type: "IDENTIFIER", Value: "db.foo"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsDBDEL(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "DBDEL", Value: "DBDEL"}, {Type: "FIELD", Value: "db"}, {Type: "PIPE", Value: "|"}, {Type: "DBDEL", Value: "DBDEL"}, {Type: "FIELD", Value: "db"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsDBDELErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "DBDEL", Value: "DBDEL"}, {Type: "INT", Value: "42"}, {Type: "PIPE", Value: "|"}, {Type: "DBDEL", Value: "DBDEL"}, {Type: "FIELD", Value: "db"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestBuildActionsCOLDEL(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	var expectedError error
	expectedError = nil

	tokens := []Token{{Type: "COLDEL", Value: "COLDEL"}, {Type: "IDENTIFIER", Value: "db.foo"}, {Type: "PIPE", Value: "|"}, {Type: "COLDEL", Value: "COLDEL"}, {Type: "IDENTIFIER", Value: "db.foo"}}

	_, err := buildActions(tokens, patterns)
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestBuildActionsCOLDELErr(t *testing.T) {
	os.Setenv("CERES_CONFIG", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{{Type: "COLDEL", Value: "COLDEL"}, {Type: "INT", Value: "42"}, {Type: "PIPE", Value: "|"}, {Type: "COLDEL", Value: "COLDEL"}, {Type: "IDENTIFIER", Value: "db.foo"}}

	_, err := buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "error")
	}
}

func TestParse(t *testing.T) {
	text := "GET db.col foo, bar | DELETE db.col [\"foo\",\"bar\"] | POST db.col {\"foo\": \"bar\"}"
	_, err := Parse(text)

	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, nil)
	}
}
