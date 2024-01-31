package utils

import "testing"

func TestIndex(t *testing.T) {
	var inputs = []map[string]interface{}{
		{
			"slice": []string{"a", "b", "c", "d", "e"},
			"value": "c",
		},
		{
			"slice": []string{"a", "b", "c", "d", "e"},
			"value": "f",
		},
	}
	var expected = []int{
		2,
		-1,
	}

	for idx, test := range inputs {
		s := test["slice"].([]string)
		v := test["value"].(string)
		out := Index(s, v)
		if out != expected[idx] {
			t.Errorf("index value mismatch: got: %d, want: %d", out, expected[idx])
		}
	}
}

func TestContains(t *testing.T) {
	var inputs = []map[string]interface{}{
		{
			"slice": []string{"a", "b", "c", "d", "e"},
			"value": "c",
		},
		{
			"slice": []string{"a", "b", "c", "d", "e"},
			"value": "f",
		},
	}
	var expected = []bool{
		true,
		false,
	}

	for idx, test := range inputs {
		s := test["slice"].([]string)
		v := test["value"].(string)
		out := Contains(s, v)
		if out != expected[idx] {
			t.Errorf("contains value mismatch: got: %v, want: %v", out, expected[idx])
		}
	}
}

func TestRemove(t *testing.T) {
	var inputs = []map[string]interface{}{
		{
			"slice": []string{"a", "b", "c", "d", "e"},
			"value": "c",
		},
		{
			"slice": []string{"a", "b", "c", "d", "e"},
			"value": "f",
		},
	}
	var expected = [][]string{
		{"a", "b", "d", "e"},
		{"a", "b", "c", "d", "e"},
	}

	for idx, test := range inputs {
		s := test["slice"].([]string)
		v := test["value"].(string)
		out := Remove(s, v)
		for jdx, val := range out {
			if val != expected[idx][jdx] {
				t.Errorf("remove value mismatch: got: %v, want: %v", out, expected[idx])
			}
		}
	}
}

func TestRemoveDupilcateValues(t *testing.T) {
	var inputs = [][]string{
		{"a", "b", "c", "d", "e", "e"},
		{"a", "b", "c", "d", "e"},
	}
	var expected = [][]string{
		{"a", "b", "c", "d", "e"},
		{"a", "b", "c", "d", "e"},
	}

	for idx, test := range inputs {
		out := RemoveDuplicateValues(test)
		for jdx, val := range out {
			if val != expected[idx][jdx] {
				t.Errorf("remove duplicate value mismatch: got: %v, want: %v", out, expected[idx])
			}
		}
	}
}

func TestHashAndSalt(t *testing.T) {
	if _, err := HashAndSalt([]byte("password")); err != nil {
		t.Errorf("error hashing password: %s", err.Error())
	}
}

func TestAndLists(t *testing.T) {
	var inputs = [][][]string{
		{
			{"a", "b", "c", "d", "e"},
			{"a", "f", "c", "g", "e"},
		},
		{
			{"a", "b", "c", "d", "e"},
			{"f", "g", "h", "i", "j"},
		},
	}
	var expected = [][]string{
		{"a", "c", "e"},
		{},
	}

	for idx, test := range inputs {
		out := AndLists(test[0], test[1])
		for jdx, val := range out {
			if val != expected[idx][jdx] {
				t.Errorf("and list value mismatch: got: %v, want: %v", out, expected[idx])
			}
		}
	}
}

func TestNotLists(t *testing.T) {
	var inputs = [][][]string{
		{
			{"a", "b", "c", "d", "e"},
			{"a", "f", "c", "g", "e"},
		},
		{
			{"a", "b", "c", "d", "e"},
			{"a", "b", "c", "d", "e"},
		},
	}
	var expected = [][]string{
		{"b", "d", "f", "g"},
		{},
	}

	for idx, test := range inputs {
		out := NotLists(test[0], test[1])
		for jdx, val := range out {
			if val != expected[idx][jdx] {
				t.Errorf("not list value mismatch: got: %v, want: %v", out, expected[idx])
			}
		}
	}
}

func TestOrLists(t *testing.T) {
	var inputs = [][][]string{
		{
			{"a", "b", "c", "d", "e"},
			{"a", "f", "c", "g", "e"},
		},
		{
			{"a", "b", "c", "d", "e"},
			{"f", "g", "h", "i", "j"},
		},
	}
	var expected = [][]string{
		{"a", "b", "c", "d", "e", "f", "g"},
		{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
	}

	for idx, test := range inputs {
		out := OrLists(test[0], test[1])
		for jdx, val := range out {
			if val != expected[idx][jdx] {
				t.Errorf("or list value mismatch: got: %v, want: %v", out, expected[idx])
			}
		}
	}
}

func TestCompareSlicePattern(t *testing.T) {
	var inputs = [][][]string{
		{
			{"a", "b", "c", "d", "e"},
			{"", "b", "", "d", ""},
		},
		{
			{"f", "g", "h", "h", "j"},
			{"a", "", "c", "", "e"},
		},
	}
	var expected = []bool{
		true,
		false,
	}

	for idx, test := range inputs {
		out := CompareSlicePattern(test[0], test[1])
		if out != expected[idx] {
			t.Errorf("compare slice pattern value mismatch: got: %v, want: %v", out, expected[idx])
		}
	}
}
