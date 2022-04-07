package utils

import (
	"bufio"
	"os"
	"reflect"
	"testing"
)

func TestBuildRangeBlocks(t *testing.T) {
	inData := []int{0, 1, 2, 3, 5, 6, 7, 9, 10}
	expected := [][]int{{0, 3}, {5, 7}, {9, 10}}

	actual := BuildRangeBlocks(inData)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Range blocks were incorrect, got: %v, want: %v", actual, expected)
	}
}

func TestCombineRangeBlocks(t *testing.T) {
	inDataLeft := [][]int{{0, 5}, {10, 16}}
	inDataRight := [][]int{{4, 12}, {20, 24}}
	expected := [][]int{{0, 16}, {20, 24}}

	actual := CombineRangeBlocks(inDataLeft, inDataRight)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Range blocks were incorrect, got: %v, want: %v", actual, expected)
	}
}

func TestBlockSort(t *testing.T) {
	inData := [][]int{{5, 7}, {0, 3}, {9, 12}, {9, 10}}
	expected := [][]int{{0, 3}, {5, 7}, {9, 10}, {9, 12}}

	BlockSort(inData)
	if !reflect.DeepEqual(inData, expected) {
		t.Errorf("Sorted blocks were incorrect, got: %v, want: %v", inData, expected)
	}
}

func TestReadLine(t *testing.T) {
	expectedLine := "{\"foo\":\"bar\",\".id\":\"bar.0\"}"
	var expectedError error
	expectedError = nil

	f, _ := os.Open("../../test/.ceres/data/db1/foo/bar")
	r := bufio.NewReader(f)
	line, err := ReadLine(r)

	if err != expectedError {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, expectedError)
	}
	if line != expectedLine {
		t.Errorf("Data was incorrect, got: %v, want: %v", line, expectedLine)
	}
}

func TestContains(t *testing.T) {
	s := []string{"foo", "bar"}
	containedT := Contains(s, "foo")
	if containedT != true {
		t.Errorf("Contained was incorrect, got: %v, want: %v", containedT, true)
	}

	containedF := Contains(s, "baz")
	if containedF != false {
		t.Errorf("Contained was incorrect, got: %v, want: %v", containedF, false)
	}
}
