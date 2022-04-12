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
	value = "COUNT"
	determineType(value, &tok)
	if tok.Type != "COUNT" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "COUNT")
	}
	value = "RECORD"
	determineType(value, &tok)
	if tok.Type != "RESOURCE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "RESOURCE")
	}
	value = "DATABASE"
	determineType(value, &tok)
	if tok.Type != "RESOURCE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "RESOURCE")
	}
	value = "COLLECTION"
	determineType(value, &tok)
	if tok.Type != "RESOURCE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "RESOURCE")
	}
	value = "USER"
	determineType(value, &tok)
	if tok.Type != "RESOURCE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "RESOURCE")
	}
	value = "PERMIT"
	determineType(value, &tok)
	if tok.Type != "RESOURCE" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "RESOURCE")
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
	value = "(nested)"
	determineType(value, &tok)
	if tok.Type != "NESTED" {
		t.Errorf("Token type was incorrect, got: %v, want: %v", tok.Type, "NESTED")
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
	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	expectedData := "^FILTER (?:LOGIC )?(?:(?:(?:LOGIC )?FIELD OP (?:STRING|INT|FLOAT|BOOL))|NESTED)(?: (?:LOGIC (?:LOGIC )?FIELD OP (?:STRING|INT|FLOAT|BOOL))|NESTED)*$"
	var expectedError error
	expectedError = nil

	patterns, err := getPatterns()

	if patterns["FILTER"] != expectedData {
		t.Errorf("Pattern was incorrect, got: %v, want: %v", patterns["FILTER"], expectedData)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want %v", err, expectedError)
	}

	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config-no-aql.json")
	config.ReadConfigFile()

	var expectedError1 error
	expectedError1 = &os.PathError{}

	_, err1 := getPatterns()

	if !errors.As(err1, &expectedError1) {
		t.Errorf("Error was incorrect, got: %v, want %v", err1, expectedError1)
	}

	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres-bad-aql/config/config.json")
	config.ReadConfigFile()

	var expectedError2 error
	expectedError2 = &json.SyntaxError{}

	_, err2 := getPatterns()

	if !errors.As(err2, &expectedError2) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err2, expectedError2)
	}
}

func TestCheckPattern(t *testing.T) {
	err := checkPattern("foo bar", "[f]oo bar", "[f]oo bar")
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, nil)
	}

	err1 := checkPattern("boo bar", "[f]oo bar", "[f]oo bar")
	if err1 == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err1, nil)
	}
}

func TestHandleConditionals(t *testing.T) {
	tokens := []Token{{Type: "LOGIC", Value: "AND"}, {Type: "STRING", Value: "foobar"}, {Type: "STRING", Value: "foobar"}}
	node := handleConditionals(tokens)

	if node.Value != "AND" {
		t.Errorf("Incorrect node value, got: %v, want: %v", node.Value, "AND")
	}

	tokens1 := []Token{{Type: "STRING", Value: "foobar"}, {Type: "OP", Value: ">"}, {Type: "STRING", Value: "foobar"}}
	node1 := handleConditionals(tokens1)

	if node1.Value != ">" {
		t.Errorf("Incorrect node value, got: %v, want: %v", node1.Value, ">")
	}

	tokens2 := []Token{{Type: "STRING", Value: "baz"}, {Type: "LOGIC", Value: "AND"}, {Type: "NESTED", Value: "(foo > bar)"}}
	node2 := handleConditionals(tokens2)

	if node2.Value != "AND" {
		t.Errorf("Incorrect node value, got: %v, want: %v", node2.Value, "AND")
	}

	tokens3 := []Token{{Type: "STRING", Value: "foo"}, {Type: "OP", Value: ">"}, {Type: "STRING", Value: "bar"}, {Type: "LOGIC", Value: "AND"}, {Type: "STRING", Value: "foo"}, {Type: "OP", Value: ">"}, {Type: "STRING", Value: "baz"}}
	node3 := handleConditionals(tokens3)

	if node3.Value != "AND" {
		t.Errorf("Incorrect node value, got: %v, want: %v", node3.Value, "AND")
	}

	tokens4 := []Token{{Type: "NESTED", Value: "(foo > bar)"}, {Type: "LOGIC", Value: "AND"}, {Type: "STRING", Value: "foo"}, {Type: "OP", Value: ">"}, {Type: "STRING", Value: "bar"}}
	node4 := handleConditionals(tokens4)

	if node4.Value != "AND" {
		t.Errorf("Incorrect node value, got: %v, want: %v", node4.Value, "AND")
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

	var action1 Action
	var expectedData1 []map[string]interface{}
	expectedData1 = nil
	var expectedError1 error
	expectedError1 = &json.SyntaxError{}

	token1 := Token{Type: "LIST", Value: "[{\"foo\":\"bar\"}"}
	err1 := handleData(token1, &action1)

	if !reflect.DeepEqual(action1.Data, expectedData1) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action1.Data, expectedData1)
	}
	if !errors.As(err1, &expectedError1) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err1, expectedError1)
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

	var action1 Action
	var expectedData1 []map[string]interface{}
	expectedData1 = nil
	var expectedError1 error
	expectedError1 = &json.SyntaxError{}

	token1 := Token{Type: "DICT", Value: "{\"foo\":\"bar\""}
	err1 := handleData(token1, &action1)

	if !reflect.DeepEqual(action1.Data, expectedData1) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action1.Data, expectedData1)
	}
	if !errors.As(err1, &expectedError1) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err1, expectedError1)
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

	var action1 Action
	var expectedData1 []string
	expectedData1 = nil
	var expectedError1 error
	expectedError1 = &json.SyntaxError{}

	token1 := Token{Type: "LIST", Value: "[\"foo\",\"bar\""}
	err1 := handleIDs(token1, &action1)

	if !reflect.DeepEqual(action1.IDs, expectedData1) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action1.IDs, expectedData1)
	}
	if !errors.As(err1, &expectedError1) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err1, expectedError1)
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

func TestHandleFields(t *testing.T) {
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

	var action1 Action
	var expectedData1 []string
	expectedData1 = nil
	var expectedError1 error
	expectedError1 = &json.SyntaxError{}

	token1 := Token{Type: "LIST", Value: "[\"foo\",\"bar\""}
	err1 := handleFields(token1, &action1)

	if !reflect.DeepEqual(action1.Fields, expectedData1) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action1.Fields, expectedData1)
	}
	if !errors.As(err1, &expectedError1) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err1, expectedError1)
	}

	var action2 Action
	expectedData2 := []string{"foo"}
	var expectedError2 error
	expectedError2 = nil

	token2 := Token{Type: "STRING", Value: "foo"}
	err2 := handleFields(token2, &action2)

	if !reflect.DeepEqual(action2.Fields, expectedData2) {
		t.Errorf("Data was incorrect, got: %v, want: %v", action2.Fields, expectedData2)
	}
	if err2 != expectedError2 {
		t.Errorf("Error was incorrect, got: %v, want: %v", err2, expectedError2)
	}
}

func TestBuildActions(t *testing.T) {
	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	patterns, _ := getPatterns()

	tokens := []Token{
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DASH", Value: "-"},
		{Type: "PIPE", Value: "|"},
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DASH", Value: "-"},
	}

	_, err := buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "FOOBAR"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\"bar\"]"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "INT", Value: "10"},
		{Type: "LIST", Value: "[\"foo\",\"bar\"]"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "LIST", Value: "[\"foo\",\"bar\"]"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\"bar\""},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "DELETE", Value: "DELETE"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "LIST", Value: "[\"foo\",\"bar\""},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "WILDCARD", Value: "*"},
		{Type: "PIPE", Value: "|"},
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "WILDCARD", Value: "*"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "FOOBAR"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "WILDCARD", Value: "*"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "INT", Value: "10"},
		{Type: "WILDCARD", Value: "*"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "WILDCARD", Value: "*"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\""},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "LIST", Value: "[\"foo\",\""},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DASH", Value: "-"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
		{Type: "PIPE", Value: "|"},
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DASH", Value: "-"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "RECORD"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "FOOBAR"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\"bar\"]"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\"bar\"]"},
		{Type: "LIST", Value: "[{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\"bar\"]"},
		{Type: "DICT", Value: "{\"foo\":\"bar\""},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PATCH", Value: "PATCH"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[\"foo\",\"bar\""},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
		{Type: "PIPE", Value: "|"},
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "RECORD"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "FOOBAR"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "POST", Value: "POST"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "LIST", Value: "[{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
		{Type: "PIPE", Value: "|"},
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "RECORD"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "FOOBAR"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "DICT", Value: "{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.col"},
		{Type: "LIST", Value: "[{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "PUT", Value: "PUT"},
		{Type: "RESOURCE", Value: "USER"},
		{Type: "LIST", Value: "[{\"foo\":\"bar\"}"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "COUNT", Value: "COUNT"},
		{Type: "PIPE", Value: "|"},
		{Type: "COUNT", Value: "COUNT"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "COUNT", Value: "COUNT"},
		{Type: "STRING", Value: "foobar"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "FILTER", Value: "FILTER"},
		{Type: "FIELD", Value: "index"},
		{Type: "OP", Value: "="},
		{Type: "STRING", Value: "debug"},
		{Type: "PIPE", Value: "|"},
		{Type: "FILTER", Value: "FILTER"},
		{Type: "FIELD", Value: "index"},
		{Type: "OP", Value: "="},
		{Type: "STRING", Value: "debug"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "FILTER", Value: "FILTER"},
		{Type: "FIELD", Value: "index"},
		{Type: "FIELD", Value: "index"},
		{Type: "FIELD", Value: "index"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "LIMIT", Value: "LIMIT"},
		{Type: "INT", Value: "10"},
		{Type: "PIPE", Value: "|"},
		{Type: "LIMIT", Value: "LIMIT"},
		{Type: "INT", Value: "10"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "LIMIT", Value: "LIMIT"},
		{Type: "STRING", Value: "10"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "LIMIT", Value: "LIMIT"},
		{Type: "INT", Value: "foobar"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "ORDERASC", Value: "ORDERASC"},
		{Type: "FIELD", Value: "index"},
		{Type: "PIPE", Value: "|"},
		{Type: "ORDERASC", Value: "ORDERASC"},
		{Type: "FIELD", Value: "index"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "ORDERASC", Value: "ORDERASC"},
		{Type: "STRING", Value: "10"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	tokens = []Token{
		{Type: "ORDERDSC", Value: "ORDERDSC"},
		{Type: "FIELD", Value: "index"},
		{Type: "PIPE", Value: "|"},
		{Type: "ORDERDSC", Value: "ORDERDSC"},
		{Type: "FIELD", Value: "index"},
	}

	_, err = buildActions(tokens, patterns)
	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	tokens = []Token{
		{Type: "ORDERDSC", Value: "ORDERDSC"},
		{Type: "STRING", Value: "10"},
	}

	_, err = buildActions(tokens, patterns)
	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}

func TestParseString(t *testing.T) {
	inputString := "GET RECORD db.foo \"hello\",\"world\" (\"this\", \"is\", \"a\", \"nested\") [\"this\", \"is\", \"a\", \"list\"] {\"this\":\"is\", \"a\":\"dict\"}"
	expectedTokens := []Token{
		{Type: "GET", Value: "GET"},
		{Type: "RESOURCE", Value: "RECORD"},
		{Type: "IDENTIFIER", Value: "db.foo"},
		{Type: "STRING", Value: "hello"},
		{Type: "STRING", Value: "hello"},
		{Type: "STRING", Value: "hello\"\"world"},
		{Type: "NESTED", Value: "(\"this\", \"is\", \"a\", \"nested\")"},
		{Type: "LIST", Value: "[\"this\", \"is\", \"a\", \"list\"]"},
		{Type: "DICT", Value: "{\"this\":\"is\", \"a\":\"dict\"}"},
	}

	tokens := parseString(inputString)

	if !reflect.DeepEqual(tokens, expectedTokens) {
		t.Errorf("Tokens were incorrect, got: %v, want: %v", tokens, expectedTokens)
	}
}

func TestParse(t *testing.T) {
	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()

	inputString := "GET RECORD db.foo *"

	_, err := Parse(inputString)

	if err != nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<nil>")
	}

	inputString = "GET RECORD"

	_, err = Parse(inputString)

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}

	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config-no-aql.json")
	config.ReadConfigFile()

	_, err = Parse(inputString)

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
