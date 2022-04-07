package cursor

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestInitialize(t *testing.T) {
	Initialize(0, 16, ModeRead)

	if Index != -1 {
		t.Errorf("Mode was incorrect, got: %d, want: %d", Index, -1)
	}
	if Mode != ModeRead {
		t.Errorf("Mode was incorrect, got: %d, want: %d", Mode, ModeRead)
	}
	if LowerBound != 0 {
		t.Errorf("Mode was incorrect, got: %d, want: %d", LowerBound, 0)
	}
	if UpperBound != 16 {
		t.Errorf("Mode was incorrect, got: %d, want: %d", UpperBound, 16)
	}
}

func TestNextRead(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 0
	expectedString = ""
	json.Unmarshal([]byte("{\"foo\":\"bar\"}"), &expectedInterface)
	expectedError = nil
	expectedOperation = OpRead

	var inString string
	var inInterface map[string]interface{}
	inString = "{\"foo\":\"bar\"}"

	Initialize(0, 16, ModeRead)
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextWrite(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 0
	expectedString = "{\"foo\":\"bar\"}\n"
	expectedError = nil
	expectedOperation = OpWrite

	var inString string
	var inInterface map[string]interface{}
	inString = ""
	json.Unmarshal([]byte("{\"foo\":\"bar\"}"), &inInterface)

	Initialize(0, 16, ModeWrite)
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextDelete(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 0
	expectedString = "\n"
	expectedError = nil
	expectedOperation = OpDelete

	var inString string
	var inInterface map[string]interface{}
	inString = ""

	Initialize(0, 16, ModeDelete)
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextWrite2(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 0
	expectedString = "{\"foo\":\"bar\",\"hello\":\"world\"}\n"
	expectedError = nil
	expectedOperation = OpWrite
	json.Unmarshal([]byte("{\"foo\":\"baz\",\"hello\":\"world\"}"), &expectedInterface)

	var inString string
	var inInterface map[string]interface{}
	inString = "{\"foo\":\"baz\",\"hello\":\"world\"}"
	json.Unmarshal([]byte("{\"foo\":\"bar\"}"), &inInterface)

	Initialize(0, 16, ModePatch)
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextNext(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 17
	expectedString = ""
	expectedError = nil
	expectedOperation = OpNext

	var inString string
	var inInterface map[string]interface{}
	inString = ""

	Initialize(0, 16, ModeRead)
	Index = 16
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextJump(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 18
	expectedString = ""
	expectedError = nil
	expectedOperation = OpJump

	var inString string
	var inInterface map[string]interface{}
	inString = ""

	Initialize(0, 16, ModeRead)
	Index = 17
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextWriteErr(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedOperation IOOp

	expectedIndex = 0
	expectedString = ""
	expectedOperation = OpError
	expectedError := &json.UnsupportedValueError{}

	var inString string
	var inInterface map[string]interface{}
	inInterface = map[string]interface{}{"foo": math.Inf(1)}

	Initialize(0, 16, ModeWrite)
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
}

func TestNextReadErr(t *testing.T) {
	var expectedIndex int
	var expectedString string
	var expectedInterface map[string]interface{}
	var expectedError error
	var expectedOperation IOOp

	expectedIndex = 0
	expectedString = ""
	expectedInterface = nil
	expectedError = &json.SyntaxError{}
	expectedOperation = OpError

	var inString string
	var inInterface map[string]interface{}
	inString = "{\"foo\":\"ba"

	Initialize(0, 16, ModeRead)
	op, outInterface, outString, err := Next(inString, inInterface)

	if Index != expectedIndex {
		t.Errorf("Index was incorrect, got: %d, want: %d", Index, expectedIndex)
	}
	if op != expectedOperation {
		t.Errorf("Operation was incorrect, got: %d, want: %d", op, expectedOperation)
	}
	if !reflect.DeepEqual(outInterface, expectedInterface) {
		t.Errorf("Interface was incorrect, got: %v, want: %v", outInterface, expectedInterface)
	}
	if outString != expectedString {
		t.Errorf("String was incorrect, got: %s, want: %s", outString, expectedString)
	}
	if !errors.As(err, &expectedError) {
		t.Errorf("Error was incorrect, got: %T, want: %T", err, expectedError)
	}
}

func TestAdvance(t *testing.T) {

	var expectedLowerBound int
	var expectedUpperBound int

	expectedLowerBound = 20
	expectedUpperBound = 24

	var inLowerBound int
	var inUpperBound int

	inLowerBound = 20
	inUpperBound = 24

	Initialize(0, 16, ModeRead)
	Advance(inLowerBound, inUpperBound)

	if LowerBound != expectedLowerBound {
		t.Errorf("LowerBound was incorrect, got: %d, want: %d", LowerBound, expectedLowerBound)
	}
	if UpperBound != expectedUpperBound {
		t.Errorf("LowerBound was incorrect, got: %d, want: %d", UpperBound, expectedUpperBound)
	}
}
