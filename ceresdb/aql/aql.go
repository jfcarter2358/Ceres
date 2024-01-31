package aql

import (
	"ceresdb/auth"
	"ceresdb/constants"
	"ceresdb/logger"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

/*
* ADD GROUP <group> TO USER <user>
* ADD GROUP <group> WITH PERMISSION <permission> TO <db>
* ADD GROUP <group> WITH PERMISSION <permission> TO <db>.<collection>
* ADD ROLE <role> TO USER <user>
* ADD ROLE <role> WITH PERMISSION <permission> TO <db>
* ADD ROLE <role> WITH PERMISSION <permission> TO <db>.<collection>
* ADD USER <user> WITH PERMISSION <permission> TO <db>
* ADD USER <user> WITH PERMISSION <permission> TO <db>.<collection>

* CREATE COLLECTION <collection> IN <db> WITH SCHEMA <json>
* CREATE DATABASE <db>
* CREATE USER <username> WITH PASSWORD <password>

* DELETE COLLECTION <collection> FROM <database>
* DELETE DATABASE <db>
* DELETE GROUP <group> FROM <db>
* DELETE GROUP <group> FROM <db>.<collection>
* DELETE GROUP <group> FROM USER <user>
* DELETE RECORD FROM <db>.<collection> WHERE <filter>
* DELETE ROLE <role> FROM <db>
* DELETE ROLE <role> FROM <db>.<collection>
* DELETE ROLE <role> FROM USER <user>
* DELETE USER <user>
* DELETE USER <user> FROM <db>
* DELETE USER <user> FROM <db>.<collection>

* GET COLLECTION FROM <database>
* GET DATABASE
* GET RECORD FROM <db>.<collection>
* GET RECORD FROM <db>.<collection> WHERE <filter>
  GET RECORD FROM <db>.<collection> ORDERED BY <field>
  GET RECORD FROM <db>.<collection> WHERE <filter> ORDERED BY <field>
* GET SCHEMA <collection> FROM <database>
* GET USER

* INSERT RECORD <json> INTO <db>.<collection>

* UPDATE GROUP <group> WITH PERMISSION <permission> IN <db>.<collection>
* UPDATE RECORD <db>.<collection> WITH <json to update> WHERE <filter>
* UPDATE ROLE <role> WITH PERMISSION <permission> IN <db>.<collection>
* UPDATE USER <user> WITH PERMISSION <permission> IN <db>.<collection>
* UPDATE USER <username> WITH PASSWORD <password>
*/

type Token struct {
	Verb           string `json:"verb"`
	Noun           string `json:"noun"`
	NounType       string `json:"noun_type"`
	Object         string `json:"object"`
	Location       string `json:"location"`
	LocationType   string `json:"location_type"`
	Input          string `json:"input"`
	Additional     string `json:"additional"`
	AdditionalType string `json:"additional_type"`
	Filter         string `json:"filter"`
	Order          string `json:"order"`
	OrderField     string `json:"order_field"`
	Limit          int    `json:"limit"`
	Count          bool   `json:"count"`
}

func ProcessQuery(q string, u auth.User) (interface{}, error) {
	ts := breakTokens(q)

	if len(ts) == 0 {
		return nil, fmt.Errorf("invalid query: length 0: %s", q)
	}

	flags := map[string]int{
		"location":   0,
		"filter":     0,
		"additional": 0,
		"order":      0,
		"limit":      0,
		"migrate":    0,
		"noun":       0,
	}

	tok := Token{}
	for _, t := range ts {
		switch strings.ToUpper(t) {
		// case constants.STATEMENT_SEP:
		// 	toks = append(toks, tok)
		// 	tok = Token{}
		case constants.PREPOSITION_IN:
			flags["location"] = -(flags["location"] - 1)
			flags["noun"] = 0
		case constants.PREPOSITION_INTO:
			flags["location"] = -(flags["location"] - 1)
		case constants.PREPOSITION_FROM:
			flags["location"] = -(flags["location"] - 1)
			flags["noun"] = 0
		case constants.PREPOSITION_TO:
			flags["location"] = -(flags["location"] - 1)
		case constants.PREPOSITION_WHERE:
			flags["filter"] = -(flags["filter"] - 1)
		case constants.PREPOSITION_WITH:
			flags["additional"] = -(flags["additional"] - 1)
		case constants.VERB_ADD, constants.VERB_CREATE, constants.VERB_DELETE, constants.VERB_GET, constants.VERB_INSERT:
			tok.Verb = strings.ToUpper(t)
		case constants.VERB_ORDER:
			flags["order"] = -(flags["order"] - 1)
		case constants.VERB_COUNT:
			tok.Count = true
		case constants.VERB_MIGRATE, constants.VERB_OUTPUT:
			continue
		case constants.VERB_LIMIT:
			flags["limit"] = -(flags["limit"] - 1)
		case constants.ADJECTIVE_ASCENDING, constants.ADJECTIVE_DESCENDING:
			if flags["order"] == 1 {
				tok.Order = strings.ToUpper(t)
				continue
			}
		case constants.NOUN_COLLECTION, constants.NOUN_DATABASE, constants.NOUN_GROUP, constants.NOUN_PASSWORD, constants.NOUN_PERMISSION, constants.NOUN_RECORD, constants.NOUN_ROLE, constants.NOUN_SCHEMA, constants.NOUN_USER:
			if flags["additional"] == 1 {
				tok.AdditionalType = strings.ToUpper(t)
				continue
			}
			if flags["location"] == 1 {
				tok.LocationType = t
				continue
			}
			tok.NounType = strings.ToUpper(t)
			flags["noun"] = -(flags["noun"] - 1)
		default:
			if flags["noun"] == 1 && checkFlags(flags) {
				tok.Noun = t
				flags["noun"] = 0
				continue
			}
			if flags["order"] == 1 && checkFlags(flags) {
				tok.OrderField = t
				flags["order"] = 0
				continue
			}
			if flags["limit"] == 1 && checkFlags(flags) {
				var err error
				tok.Limit, err = strconv.Atoi(t)
				if err != nil {
					return nil, err
				}
				flags["limit"] = 0
				continue
			}
			if flags["location"] == 1 && checkFlags(flags) {
				tok.Location = t
				if tok.LocationType == "" {
					locationParts := strings.Split(tok.Location, ".")
					if len(locationParts) == 1 {
						tok.LocationType = constants.NOUN_DATABASE
					} else if len(locationParts) == 2 {
						tok.LocationType = constants.NOUN_COLLECTION
					}
				}
				flags["location"] = 0
				continue
			}
			if flags["filter"] == 1 && checkFlags(flags) {
				tok.Filter = t
				flags["filter"] = 0
				continue
			}
			if flags["additional"] == 1 && checkFlags(flags) {
				tok.Additional = t
				flags["additional"] = 0
				continue
			}
		}
	}

	tt, _ := json.Marshal(tok)

	logger.Debugf("", "Query: %v", string(tt))

	switch tok.Verb {
	case constants.VERB_ADD:
		err := processAdd(tok, u)
		return nil, err
	case constants.VERB_CREATE:
		err := processCreate(tok, u)
		return nil, err
	case constants.VERB_DELETE:
		err := processDelete(tok, u)
		return nil, err
	case constants.VERB_GET:
		out, err := processGet(tok, u)
		return out, err
	case constants.VERB_INSERT:
		err := processInsert(tok, u)
		return nil, err
	case constants.VERB_UPDATE:
		err := processUpdate(tok, u)
		return nil, err
	}
	return nil, fmt.Errorf("invalid query: unknown verb: %s", tok.Verb)
}

func checkFlags(flags map[string]int) bool {
	total := 0
	for _, v := range flags {
		total += v
	}

	return total == 1
}

func breakTokens(queryString string) []string {
	output := []string{}
	buffer := ""
	braceDepth := 0
	parenDepth := 0
	quoteDepth := 0
	bracketDepth := 0
	runes := []rune(queryString)
	lookBehind := ' '
	for _, r := range runes {
		if lookBehind != '\\' {
			switch r {
			case '{':
				if parenDepth+quoteDepth+bracketDepth == 0 {
					braceDepth += 1
				}
				buffer += string(r)
			case '}':
				if parenDepth+quoteDepth+bracketDepth == 0 {
					braceDepth -= 1
				}
				buffer += string(r)
			case '(':
				if braceDepth+quoteDepth+bracketDepth == 0 {
					parenDepth += 1
				}
				buffer += string(r)
			case ')':
				if braceDepth+quoteDepth+bracketDepth == 0 {
					parenDepth -= 1
				}
				buffer += string(r)
			case '[':
				if parenDepth+quoteDepth+braceDepth == 0 {
					bracketDepth += 1
				}
				buffer += string(r)
			case ']':
				if parenDepth+quoteDepth+braceDepth == 0 {
					bracketDepth -= 1
				}
				buffer += string(r)
			case '"':
				if parenDepth+braceDepth+bracketDepth == 0 {
					quoteDepth = -(quoteDepth - 1)
				}
				buffer += string(r)
			case ' ':
				if parenDepth+braceDepth+bracketDepth+quoteDepth == 0 {
					output = append(output, buffer)
					buffer = ""
				}
			case '|':
				if parenDepth+braceDepth+bracketDepth+quoteDepth == 0 {
					output = append(output, buffer)
					output = append(output, "|")
					buffer = ""
				}
			default:
				buffer += string(r)
			}
			continue
		}
		buffer += string(r)
	}
	if len(buffer) > 0 {
		output = append(output, buffer)
	}
	return output
}
