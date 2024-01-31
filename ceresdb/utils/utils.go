// utils.go

package utils

import (
	"ceresdb/constants"
	"ceresdb/logger"
	"fmt"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

func Error(err error, c *gin.Context, statusCode int) {
	logger.Error("", err.Error())
	c.JSON(statusCode, gin.H{"error": err.Error()})
}

func Index(s []string, e string) int {
	for idx, a := range s {
		if a == e {
			return idx
		}
	}
	return -1
}

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func Remove(s []string, e string) []string {
	for i, a := range s {
		if a == e {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func RemoveDuplicateValues(stringSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}

	// If the key(values of the slice) is not equal
	// to the already present value in new slice (list)
	// then we append it. else we jump on another element.
	for _, entry := range stringSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func HashAndSalt(pwd []byte) (string, error) {
	hash, err := bcrypt.GenerateFromPassword(pwd, bcrypt.MinCost)
	if err != nil {
		return "", nil
	}
	return string(hash), nil
}

func AndLists(a, b []string) []string {
	out := []string{}
	for _, val := range a {
		if Contains(b, val) {
			out = append(out, val)
		}
	}
	return out
}

func NotLists(a, b []string) []string {
	logger.Tracef("", "Performing not comparison between %s and %s", a, b)
	out := []string{}
	for _, val := range a {
		if Contains(b, val) {
			continue
		}
		out = append(out, val)
	}
	for _, val := range b {
		if Contains(a, val) {
			continue
		}
		out = append(out, val)
	}
	return out
}

func OrLists(a, b []string) []string {
	out := append(a, b...)
	return RemoveDuplicateValues(out)
}

func CompareSlicePattern(a, b []string) bool {
	for idx, aa := range a {
		if b[idx] == "" {
			continue
		}
		logger.Tracef("", "Comparing %s to %s", strings.ToUpper(aa), strings.ToUpper(b[idx]))
		if strings.ToUpper(aa) != strings.ToUpper(b[idx]) {
			return false
		}
	}
	return true
}

func SortInterfacesDescending(keys []string, objs []interface{}) ([]interface{}, error) {
	if len(objs) == 0 {
		return []interface{}{}, nil
	}
	dType, err := getType(keys, objs[0])
	if err != nil {
		return nil, err
	}
	outMap := []map[string]interface{}{}
	for _, obj := range objs {
		outMap = append(outMap, map[string]interface{}{
			"val": getVal(keys, obj),
			"obj": obj,
		})
	}
	logger.Tracef("", "Descending type: %s", dType)
	switch dType {
	case constants.DATATYPE_STRING:
		logger.Tracef("", "descending type string")
		sort.Slice(outMap,
			func(i, j int) bool {
				return outMap[i]["val"].(string) > outMap[j]["val"].(string)
			})
	case constants.DATATYPE_FLOAT:
		logger.Tracef("", "descending type float")
		sort.Slice(outMap,
			func(i, j int) bool {
				return outMap[i]["val"].(float64) > outMap[j]["val"].(float64)
			})
	case constants.DATATYPE_INT:
		logger.Tracef("", "descending type int")
		sort.Slice(outMap,
			func(i, j int) bool {
				return outMap[i]["val"].(int) > outMap[j]["val"].(int)
			})
	}
	out := []interface{}{}
	for _, obj := range outMap {
		out = append(out, obj["obj"])
	}
	return out, nil
}

func SortInterfacesAscending(keys []string, objs []interface{}) ([]interface{}, error) {
	if len(objs) == 0 {
		return []interface{}{}, nil
	}
	dType, err := getType(keys, objs[0])
	if err != nil {
		return nil, err
	}
	outMap := []map[string]interface{}{}
	for _, obj := range objs {
		outMap = append(outMap, map[string]interface{}{
			"val": getVal(keys, obj),
			"obj": obj,
		})
	}
	switch dType {
	case constants.DATATYPE_STRING:
		sort.Slice(outMap,
			func(i, j int) bool {
				return outMap[i]["val"].(string) < outMap[j]["val"].(string)
			})
	case constants.DATATYPE_FLOAT:
		sort.Slice(outMap,
			func(i, j int) bool {
				return outMap[i]["val"].(float64) < outMap[j]["val"].(float64)
			})
	case constants.DATATYPE_INT:
		sort.Slice(outMap,
			func(i, j int) bool {
				return outMap[i]["val"].(int) < outMap[j]["val"].(int)
			})
	}
	out := []interface{}{}
	for _, obj := range outMap {
		out = append(out, obj["obj"])
	}
	return out, nil
}

func getVal(keys []string, obj interface{}) interface{} {
	if len(keys) == 0 {
		return obj
	}
	m := obj.(map[string]interface{})
	return getVal(keys[1:], m[keys[0]])
}

func getType(keys []string, obj interface{}) (string, error) {
	if len(keys) == 0 {
		if _, ok := obj.(string); ok {
			return constants.DATATYPE_STRING, nil
		}
		// if _, ok := obj.(int); ok {
		// 	return constants.DATATYPE_STRING, nil
		// }
		if _, ok := obj.(float64); ok {
			return constants.DATATYPE_FLOAT, nil
		}
		if _, ok := obj.(int); ok {
			return constants.DATATYPE_INT, nil
		}
		return "", fmt.Errorf("cannot sort, invalid datatype of %v", obj)
	}
	m := obj.(map[string]interface{})
	return getType(keys[1:], m[keys[0]])
}
