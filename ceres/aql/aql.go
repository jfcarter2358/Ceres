// Package aql handles Antler Query Language parsing
package aql

import (
	"ceres/config"
	"ceres/utils"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Node struct {
	Value string
	Left  *Node
	Right *Node
}

type Token struct {
	Type  string
	Value string
}
type DepthStruct struct {
	Paren   int
	Brace   int
	Bracket int
}

type FlagStruct struct {
	Quote   bool
	Bracket bool
	Brace   bool
	Paren   bool
}

type Action struct {
	Type       string
	Identifier string
	IDs        []string
	Fields     []string
	Limit      int
	Filter     Node
	Order      string
	OrderDir   string
	Data       []map[string]interface{}
	User       string
}

// Determine the type of a token based on its value
func determineType(value string, token *Token) {
	ops := []string{">", ">=", "=", "<=", "<", "!="}
	logic := []string{"AND", "OR", "XOR", "NOT"}
	switch value {
	// First check the types that only have one possible value
	case ",":
		token.Type = "COMMA"
	case "(":
		token.Type = "OPEN_PAREN"
	case ")":
		token.Type = "CLOSE_PAREN"
	case "[":
		token.Type = "OPEN_BRACKET"
	case "]":
		token.Type = "CLOSE_BRACKET"
	case "{":
		token.Type = "OPEN_BRACE"
	case "}":
		token.Type = "CLOSE_BRACE"
	case "|":
		token.Type = "PIPE"
	case "*":
		token.Type = "WILDCARD"
	case "GET":
		token.Type = "GET"
	case "POST":
		token.Type = "POST"
	case "PUT":
		token.Type = "PUT"
	case "PATCH":
		token.Type = "PATCH"
	case "DELETE":
		token.Type = "DELETE"
	case "LIMIT":
		token.Type = "LIMIT"
	case "FILTER":
		token.Type = "FILTER"
	case "ORDERASC":
		token.Type = "ORDERASC"
	case "ORDERDSC":
		token.Type = "ORDERDSC"
	case "-":
		token.Type = "DASH"
	case "DBADD":
		token.Type = "DBADD"
	case "COLADD":
		token.Type = "COLADD"
	case "COLMOD":
		token.Type = "COLMOD"
	case "DBDEL":
		token.Type = "DBDEL"
	case "COLDEL":
		token.Type = "COLDEL"
	case "PERMITADD":
		token.Type = "PERMITADD"
	case "PERMITDEL":
		token.Type = "PERMITDEL"
	case "PERMITMOD":
		token.Type = "PERMITMOD"
	case "PERMITGET":
		token.Type = "PERMITGET"
	case "USERADD":
		token.Type = "USERADD"
	case "USERDEL":
		token.Type = "USERDEL"
	case "USERMOD":
		token.Type = "USERMOD"
	case "USERGET":
		token.Type = "USERGET"
	case "COUNT":
		token.Type = "COUNT"
	default:
		// Check the more open-ended types
		if utils.Contains(ops, value) {
			token.Type = "OP"
		} else if utils.Contains(logic, value) {
			token.Type = "LOGIC"
		} else if value[0:1] == "\"" {
			token.Type = "STRING"
			token.Value = value[1 : len(value)-1]
		} else if value[0:1] == "[" {
			token.Type = "LIST"
		} else if value[0:1] == "{" {
			token.Type = "DICT"
		} else if value[0:1] == "(" {
			token.Type = "NESTED"
		} else if res, _ := regexp.MatchString("^-?\\d+$", value); res {
			token.Type = "INT"
		} else if res, _ := regexp.MatchString("^-?\\d+\\.\\d+$", value); res {
			token.Type = "FLOAT"
		} else if res, _ := regexp.MatchString("^(?:true|false)$", value); res {
			token.Type = "BOOLEAN"
		} else if res, _ := regexp.MatchString("^[a-zA-Z0-9-_]+\\.[a-zA-Z0-9-_]+$", value); res {
			token.Type = "IDENTIFIER"
		} else {
			token.Type = "FIELD"
		}
	}
}

// Get the patterns which define the AQL language
func getPatterns() (map[string]string, error) {
	path := config.Config.CeresDir + "/config/aql.json"

	// Open our jsonFile
	jsonFile, err := os.Open(path)

	// If we os.Open returns an error then handle it
	if err != nil {
		return nil, err
	}

	// Read and unmarshal the JSON
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var f interface{}

	// Read the JSON
	err = json.Unmarshal(byteValue, &f)
	if err != nil {
		return nil, err
	}

	output := make(map[string]string)

	for key, val := range f.(map[string]interface{}) {
		output[key] = val.(string)
	}

	return output, nil
}

// Check a provided action against the AQL grammar to ensure that it is syntactically correct
func checkPattern(actionString, actionSyntax, pattern string) error {
	if res, _ := regexp.MatchString(pattern, actionString); !res {
		err := errors.New("Invalid syntax: `" + actionSyntax + "`. Got: " + actionString + ", want: " + pattern)
		return err
	}
	return nil
}

// func printFilter(node Node, indent string) {
// 	if indent == "" {
// 		fmt.Print("HEAD  : ")
// 	}
// 	fmt.Printf("%v%v\n", indent, node.Value)
// 	if node.Left != nil {
// 		fmt.Print("LEFT  : ")
// 		printFilter(*node.Left, indent+"  ")
// 	}
// 	if node.Right != nil {
// 		fmt.Print("RIGHT : ")
// 		printFilter(*node.Right, indent+"  ")
// 	}
// }

// Given a list of filter tokens, produce a tree that represents the logical and conditional operations
func handleConditionals(tokens []Token) Node {
	nodes := make([]Node, 0)
	LOGIC := []string{"AND", "OR", "XOR"}

	// Break down the tokens into a list of nodes with operations containing their operands on the
	// left and right sides
	idx := 0
	for idx < len(tokens) {
		if tokens[idx].Type == "OP" {
			nodeL := Node{Value: tokens[idx-1].Value}
			nodeR := Node{Value: tokens[idx+1].Value}
			nodeC := Node{Value: tokens[idx].Value, Left: &nodeL, Right: &nodeR}
			nodes = append(nodes, nodeC)
			idx++
		} else if utils.Contains(LOGIC, tokens[idx].Value) || tokens[idx].Value == "NOT" || tokens[idx].Type == "NESTED" {
			node := Node{Value: tokens[idx].Value}
			nodes = append(nodes, node)
		}
		idx++
	}

	var head *Node
	head = &Node{}
	firstPass := true

	// Loop through the nodes and build the tree
	for idx := range nodes {
		if utils.Contains(LOGIC, nodes[idx].Value) {
			nodes[idx].Left = head
			head = &nodes[idx]
		} else {
			if strings.HasPrefix(nodes[idx].Value, "(") {
				// If we come across a nested statement then we want to parse that as its own thing
				// and then just stick the resulting tree in as a node
				parsed := parseString(nodes[idx].Value[1 : len(nodes[idx].Value)-1])
				node := handleConditionals(parsed)
				if firstPass == false {
					head.Right = &node
				} else {
					head = &node
				}
			} else {
				if firstPass == false {
					head.Right = &nodes[idx]
				} else {
					head = &nodes[idx]
				}
			}
		}
		firstPass = false
	}

	return *head
}

// Parse a list or dictionary into the action's data field
func handleData(token Token, currentAction *Action) error {
	if token.Type == "LIST" {
		var d interface{}
		err := json.Unmarshal([]byte(token.Value), &d)
		if err != nil {
			return err
		}
		tmp := make([]map[string]interface{}, 0)
		for _, v := range d.([]interface{}) {
			tmp = append(tmp, v.(map[string]interface{}))
		}
		currentAction.Data = tmp
	} else {
		var d interface{}
		err := json.Unmarshal([]byte(token.Value), &d)
		if err != nil {
			return err
		}
		tmp := make([]map[string]interface{}, 1)
		tmp[0] = d.(map[string]interface{})
		currentAction.Data = tmp
	}
	return nil
}

// Parse a string or list into the action's ID field
func handleIDs(token Token, currentAction *Action) error {
	if token.Type == "LIST" {
		var d interface{}
		err := json.Unmarshal([]byte(token.Value), &d)
		if err != nil {
			return err
		}
		tmp := make([]string, 0)
		for _, v := range d.([]interface{}) {
			tmp = append(tmp, v.(string))
		}
		currentAction.IDs = tmp
	} else {
		currentAction.IDs = []string{token.Value}
	}
	return nil
}

// Parse a string or list into the action's fields field
func handleFields(token Token, currentAction *Action) error {
	if token.Type == "LIST" {
		var d interface{}
		err := json.Unmarshal([]byte(token.Value), &d)
		if err != nil {
			return err
		}
		tmp := make([]string, 0)
		for _, v := range d.([]interface{}) {
			tmp = append(tmp, v.(string))
		}
		currentAction.Fields = tmp
	} else {
		currentAction.Fields = []string{token.Value}
	}
	return nil
}

// buildActions takes a list of tokens and figures out which actions should be created to operate
// within Ceres.
func buildActions(tokens []Token, patterns map[string]string) ([]Action, error) {
	tokenActions := make([][]Token, 0)
	buffer := make([]Token, 0)
	// Break the list of tokens down into a list of sepearate actions that were originally separated
	// by pipe characters
	for _, token := range tokens {
		if token.Type != "PIPE" {
			buffer = append(buffer, token)
		} else {
			tokenActions = append(tokenActions, buffer)
			buffer = make([]Token, 0)
		}
	}
	tokenActions = append(tokenActions, buffer)
	actions := make([]Action, 0)

	currentAction := Action{}
	firstFlag := true

	// Look through each action list and build/modify the action object from it
	// TODO: break each "case" out into its own function for readability/maintainability
	for _, tokenAction := range tokenActions {
		command := tokenAction[0]
		actionString := ""
		actionSyntax := ""
		for _, token := range tokenAction {
			actionString += token.Type + " "
			if token.Type != "STRING" {
				actionSyntax += token.Value
			} else {
				actionSyntax += "\"" + token.Value + "\""
			}
			actionSyntax += " "
		}
		actionString = actionString[:len(actionString)-1]
		actionSyntax = actionSyntax[:len(actionSyntax)-1]

		switch command.Type {
		case "GET":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["GET"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "GET"}
			currentAction.Identifier = tokenAction[1].Value

			if len(tokenAction) > 2 {
				if err := handleFields(tokenAction[2], &currentAction); err != nil {
					return nil, err
				}
			}

			firstFlag = false
		case "COUNT":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["COUNT"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "COUNT"}
			currentAction.Identifier = tokenAction[1].Value

			firstFlag = false
		case "POST":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["POST"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "POST"}
			currentAction.Identifier = tokenAction[1].Value

			if err := handleData(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "PUT":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["PUT"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "PUT"}
			currentAction.Identifier = tokenAction[1].Value

			if err := handleData(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "PATCH":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["PATCH"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "PATCH"}
			currentAction.Identifier = tokenAction[1].Value

			if err := handleIDs(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}
			if err := handleData(tokenAction[3], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "DELETE":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["DELETE"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "DELETE"}
			currentAction.Identifier = tokenAction[1].Value

			if err := handleIDs(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "FILTER":
			if err := checkPattern(actionString, actionSyntax, patterns["FILTER"]); err != nil {
				return nil, err
			}
			currentAction.Filter = handleConditionals(tokenAction[1:])
		case "LIMIT":
			if err := checkPattern(actionString, actionSyntax, patterns["LIMIT"]); err != nil {
				return nil, err
			}
			val, err := strconv.Atoi(tokenAction[1].Value)
			if err != nil {
				return nil, err
			}
			currentAction.Limit = val
		case "ORDERASC":
			if err := checkPattern(actionString, actionSyntax, patterns["ORDERASC"]); err != nil {
				return nil, err
			}
			currentAction.OrderDir = "ASC"
			currentAction.Order = tokenAction[1].Value
		case "ORDERDSC":
			if err := checkPattern(actionString, actionSyntax, patterns["ORDERDSC"]); err != nil {
				return nil, err
			}
			currentAction.OrderDir = "DSC"
			currentAction.Order = tokenAction[1].Value
		case "DBADD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["DBADD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "DBADD"}
			currentAction.Identifier = tokenAction[1].Value

			firstFlag = false
		case "COLADD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["COLADD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "COLADD"}
			currentAction.Identifier = tokenAction[1].Value

			if err := handleData(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "COLMOD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["COLMOD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "COLMOD"}
			currentAction.Identifier = tokenAction[1].Value

			if err := handleData(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "DBDEL":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["DBDEL"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "DBDEL"}
			currentAction.Identifier = tokenAction[1].Value

			firstFlag = false
		case "COLDEL":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["COLDEL"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "COLDEL"}
			currentAction.Identifier = tokenAction[1].Value

			firstFlag = false
		case "USERADD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["USERADD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "USERADD"}
			if err := handleData(tokenAction[1], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "USERMOD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["USERMOD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "USERMOD"}
			if err := handleData(tokenAction[1], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "USERDEL":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["USERDEL"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "USERDEL"}
			currentAction.User = tokenAction[1].Value

			firstFlag = false
		case "USERGET":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["USERGET"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "USERGET"}
			currentAction.User = tokenAction[1].Value

			firstFlag = false
		case "PERMITADD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["PERMITADD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "PERMITADD"}
			currentAction.Identifier = tokenAction[1].Value
			if err := handleData(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "PERMITMOD":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["PERMITMOD"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "PERMITMOD"}
			currentAction.Identifier = tokenAction[1].Value
			if err := handleData(tokenAction[2], &currentAction); err != nil {
				return nil, err
			}

			firstFlag = false
		case "PERMITDEL":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["PERMITDEL"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "PERMITDEL"}
			currentAction.Identifier = tokenAction[1].Value
			currentAction.User = tokenAction[2].Value

			firstFlag = false
		case "PERMITGET":
			if !firstFlag {
				actions = append(actions, currentAction)
			}

			if err := checkPattern(actionString, actionSyntax, patterns["PERMITGET"]); err != nil {
				return nil, err
			}
			currentAction = Action{Type: "PERMITGET"}
			currentAction.Identifier = tokenAction[1].Value
			currentAction.User = tokenAction[2].Value

			firstFlag = false
		}
	}
	actions = append(actions, currentAction)
	return actions, nil
}

// parseString builds a list of tokens based off of a string of text.
func parseString(input string) []Token {
	text := strings.Split(input, "")
	depths := DepthStruct{Paren: 0, Brace: 0, Bracket: 0}
	flags := FlagStruct{Quote: false}
	buffer := ""
	tokens := make([]Token, 0)

	for idx, char := range text {
		look_behind := ""
		if idx > 0 {
			look_behind = text[idx-1]
		}
		if char == "\"" && look_behind != "\\" {
			flags.Quote = !flags.Quote
			buffer += char
		} else {
			if !flags.Quote {
				switch char {
				case "(":
					if depths.Paren == 0 {
						flags.Paren = true
					}
					depths.Paren += 1
					buffer += char
				case ")":
					depths.Paren -= 1
					buffer += char
					if depths.Paren == 0 {
						flags.Paren = false
					}
				case "[":
					if depths.Bracket == 0 {
						flags.Bracket = true
					}
					depths.Bracket += 1
					buffer += char
				case "]":
					depths.Bracket -= 1
					buffer += char
					if depths.Bracket == 0 {
						flags.Bracket = false
					}
				case "{":
					if depths.Brace == 0 {
						flags.Brace = true
					}
					depths.Brace += 1
					buffer += char
				case "}":
					depths.Brace -= 1
					buffer += char
					if depths.Brace == 0 {
						flags.Brace = false
					}
				case " ":
					if flags.Bracket || flags.Brace || flags.Paren {
						buffer += char
					} else {
						child := Token{Value: buffer}
						determineType(buffer, &child)
						tokens = append(tokens, child)
						buffer = ""
					}
				case ",":
					if flags.Bracket || flags.Brace || flags.Paren {
						buffer += char
					} else {
						child1 := Token{Value: buffer}
						determineType(buffer, &child1)
						tokens = append(tokens, child1)

						child2 := Token{Value: ","}
						determineType(buffer, &child2)
						tokens = append(tokens, child2)
					}
				default:
					buffer += char
				}
			} else {
				buffer += char
			}
		}
	}
	child := Token{Value: buffer}
	determineType(buffer, &child)
	tokens = append(tokens, child)

	return tokens

}

// Parse processes input and get the actions as a result.
// Actions are verified against the AQL grammar for correctness.
func Parse(input string) ([]Action, error) {
	tokens := parseString(input)
	patterns, err := getPatterns()
	if err != nil {
		return nil, err
	}
	actions, err := buildActions(tokens, patterns)
	if err != nil {
		return nil, err
	}
	return actions, nil
}
