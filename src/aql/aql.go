package aql

import (
	"fmt"
	"strings"
)

/******** SPEC ********
DATA TYPES
- string
- int
- float
- bool
- byte
- any
- list
- dict
- pattern

SYNTAX
- Retrieve data
	GET {ALL|field1,field2,...} FROM <database>.<collection>
- Update data
	UPDATE <database>.<collection> WITH <json document> WHERE <condition>
- Add data
	INSERT <json document> INTO <database>.<collection>
- Remove data
	DELETE FROM <database>.<collection> WHERE <condition>
- Remove field(s)
	DELETE <field1,field2,...> FROM <database>.<collection>
- Add field(s)
	CREATE <field1,field2,...>
- Create database
	CREATE DATABASE <database>
- Remove database
	DELETE DATABASE <database>
- Create collection
	CREATE COLLECTION IN <db> WITH SCHEMA <schema document>
- Remove collection
	DELETE COLLECTION <database>.<collection>
- Index collection
	CREATE INDEX <json document> IN <database.collection>
- Create user
	CREATE USER <username> WITH PASSWORD <password>
- Remove user
	DELETE USER <username>
- Change password
	UPDATE USER <username> WITH PASSWORD <password>
- Add user to role
	ADD USER <username> TO ROLE <role>
- Remove user from role
	DELETE USER <username> FROM ROLE <role>
- Create role
	CREATE ROLE <role> WITH PERMISSION <read|write|admin> ON <database>.<collection>
	**/

type Op struct {
	Action string
	Left   interface{}
	Right  interface{}
}

func Parse(query string) (Op, error) {
	var err error
	parts := []string{}
	op := Op{}
	trimmed := strings.Replace(query, "\n", "", -1)
	statements := strings.Split(trimmed, ";")

	for _, statement := range statements {
		parts, err = breakStatement(statement)
		if err != nil {
			return op, err
		}
		switch parts[0] {
		case "ADD":
			op, err = parseAdd(op, parts[1:])
			if err != nil {
				return op, err
			}
		case "CREATE":

		case "DELETE":

		case "GET":

		case "INSERT":

		case "UPDATE":

		default:
			return op, createError("statement", "Expected verb to be a member of the set {ADD,CREATE,DELETE,GET,INSERT,UPDATE}", "", 0, parts)
		}
	}
	return op, nil
}

func parseAdd(op Op, parts []string) (Op, error) {
	if len(parts) == 0 {
		return op, createError("statement", "Missing required parts of statement", "ADD", -1, parts)
	}
	switch parts[0] {
	case "USER":
		if len(parts) != 5 {
			return op, createError("statement", "Missing required parts of statement", "ADD", -1, parts)
		}
		if parts[2] != "TO" {
			return op, createError("syntax", "Expected 'TO' keyword", "ADD", 2, parts)
		}
		if parts[3] != "ROLE" {
			return op, createError("syntax", "Expected 'ROLE' keyword", "ADD", 3, parts)
		}
		if dType := getDatatype(parts[1]); dType != "literal" {
			return op, createError("syntax", fmt.Sprintf("Expected type 'literal', got '%s'", dType), "ADD", 1, parts)
		}
		if dType := getDatatype(parts[4]); dType != "literal" {
			return op, createError("syntax", fmt.Sprintf("Expected type 'literal', got '%s'", dType), "ADD", 4, parts)
		}
		op.Action = "ADD_USER"
	}
	return op, nil
}

func createError(errType, message, verb string, idx int, parts []string) error {
	formatString := "Invalid %s: %s | "
	args := []interface{}{errType, message}

	if idx == -1 {
		formatString += "-->"
		if verb != "" {
			formatString += "%s"
			args = append(args, verb)
		}
		if len(parts) > 0 {
			formatString += " %s"
			args = append(args, strings.Join(parts, " "))
		}
		formatString += "<--"
		return fmt.Errorf(formatString, args...)
	}

	prefix := strings.Join(parts[:idx], " ")
	suffix := strings.Join(parts[idx+1:], " ")

	if verb != "" {
		formatString += "%s "
		args = append(args, verb)
	}

	if prefix != "" {
		formatString += "%s "
		args = append(args, prefix)
	}
	formatString += "-->%s<--"
	args = append(args, parts[idx])
	if suffix != "" {
		formatString += " %s"
		args = append(args, suffix)
	}

	return fmt.Errorf(formatString, args...)
}

func getDatatype(part string) string {
	first := fmt.Sprintf("%c", part[0])

	switch first {
	case "{":
		return "dict"
	case "[":
		return "list"
	case "(":
		return "group"
	case "'":
		return "string"
	default:
		return "literal"
	}
}

func breakStatement(statement string) ([]string, error) {
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	quoteDepth := 0

	output := []string{}
	buffer := ""

	lookBehind := ""
	for _, rawChar := range statement {
		char := fmt.Sprintf("%c", rawChar)

		switch char {
		case "{":
			if bracketDepth+parenDepth+quoteDepth == 0 && lookBehind != "\\" {
				braceDepth = 1
			}
		case "}":
			if bracketDepth+parenDepth+quoteDepth == 0 && lookBehind != "\\" {
				braceDepth = 0
			}
		case "[":
			if braceDepth+parenDepth+quoteDepth == 0 && lookBehind != "\\" {
				bracketDepth = 1
			}
		case "]":
			if braceDepth+parenDepth+quoteDepth == 0 && lookBehind != "\\" {
				bracketDepth = 0
			}
		case "(":
			if braceDepth+bracketDepth+quoteDepth == 0 && lookBehind != "\\" {
				parenDepth = 1
			}
		case ")":
			if braceDepth+bracketDepth+quoteDepth == 0 && lookBehind != "\\" {
				parenDepth = 0
			}
		case "'":
			if braceDepth+bracketDepth+parenDepth == 0 && lookBehind != "\\" {
				quoteDepth = -1 * (quoteDepth - 1)
			}
		case " ":
			if braceDepth+bracketDepth+parenDepth+quoteDepth == 0 {
				output = append(output, buffer)
				buffer = ""
				continue
			}
		}
		buffer = fmt.Sprintf("%s%s", buffer, char)
	}
	output = append(output, buffer)

	if braceDepth == 1 {
		return output, fmt.Errorf("Invalid syntax: Unmatched `{` | -->%s<--", statement)
	}
	if bracketDepth == 1 {
		return output, fmt.Errorf("Invalid syntax: Unmatched `[` | -->%s<--", statement)
	}
	if parenDepth == 1 {
		return output, fmt.Errorf("Invalid syntax: Unmatched `(` | -->%s<--", statement)
	}
	if quoteDepth == 1 {
		return output, fmt.Errorf("Invalid syntax: Unmatched `'` | -->%s<--", statement)
	}
	return output, nil
}
