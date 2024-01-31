package aql

// func TestProcessAdd(t *testing.T) {
// 	queries := []string{
// 		"ADD GROUP foo TO USER ceresdb",
// 		"ADD GROUP foo TO USER foobar",
// 		"ADD GROUP foo WITH PERMISSION admin TO foo",
// 		"ADD GROUP foo WITH PERMISSION asdf TO foo",
// 		"ADD GROUP foo WITH PERMISSION admin TO foo.bar",
// 		"ADD GROUP foo WITH PERMISSION asdf TO foo.bar",
// 		"ADD ROLE foo TO USER ceresdb",
// 		"ADD ROLE foo TO USER foobar",
// 		"ADD ROLE foo WITH PERMISSION admin TO foo",
// 		"ADD ROLE foo WITH PERMISSION asdf TO foo",
// 	}
// 	expected := []bool{
// 		true,
// 		false,
// 		true,
// 		false,
// 		true,
// 		false,
// 		true,
// 		false,
// 		true,
// 		false,
// 	}

// 	id := uuid.New().String()
// 	if err := setup(id); err != nil {
// 		t.Errorf("error on setup: %s", err.Error())
// 	}

// 	for idx, q := range queries {
// 		tokens := strings.Split(q, " ")[1:]
// 		if expected[idx] {
// 			if err := processAdd(q, tokens, USER); err != nil {
// 				t.Errorf("error adding query for test %d: %s", idx, err.Error())
// 			}
// 		} else {
// 			if err := processAdd(q, tokens, USER); err == nil {
// 				t.Errorf("success when expecting failure for query %s", q)
// 			}
// 		}
// 	}

// 	if err := clean(id); err != nil {
// 		t.Errorf("error on clean: %s", err.Error())
// 	}
// }
