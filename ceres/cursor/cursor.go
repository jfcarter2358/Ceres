// cursor.go

package cursor

import (
	"encoding/json"
)

type IOMode int64
type IOOp int64

const (
	ModeRead IOMode = iota
	ModeWrite
	ModeDelete
	ModePatch
)

const (
	OpRead IOOp = iota
	OpWrite
	OpDelete
	OpNext
	OpJump
	OpError
)

var Index int
var Mode IOMode
var LowerBound int
var UpperBound int

func Initialize(lowerBound, upperBound int, mode IOMode) {
	Index = -1
	Mode = mode
	LowerBound = lowerBound
	UpperBound = upperBound
}

func Next(line string, datum map[string]interface{}) (IOOp, map[string]interface{}, string, error) {
	Index += 1
	if Index >= LowerBound && Index <= UpperBound {
		switch Mode {
		case ModeRead:
			var outInterface map[string]interface{}
			if line == "" {
				return OpRead, outInterface, "", nil
			}
			err := json.Unmarshal([]byte(line), &outInterface)
			if err != nil {
				return OpError, nil, "", err
			}
			return OpRead, outInterface, "", nil
		case ModeWrite:
			var outString string
			var outInterface map[string]interface{}
			json.Unmarshal([]byte(line), &outInterface)
			outBytes, err := json.Marshal(datum)
			if err != nil {
				return OpError, nil, "", err
			}
			outString = string(outBytes) + "\n"
			return OpWrite, outInterface, outString, nil
		case ModeDelete:
			var outString string
			var outInterface map[string]interface{}
			outString = "\n"
			json.Unmarshal([]byte(line), &outInterface)
			return OpDelete, outInterface, outString, nil
		case ModePatch:
			var outString string
			var outInterface map[string]interface{}
			tmpInterface := make(map[string]interface{})
			json.Unmarshal([]byte(line), &outInterface)
			for key, val := range outInterface {
				if _, ok := datum[key]; !ok {
					tmpInterface[key] = val
				} else {
					tmpInterface[key] = datum[key]
				}
			}
			outBytes, err := json.Marshal(tmpInterface)
			if err != nil {
				return OpError, nil, "", err
			}
			outString = string(outBytes) + "\n"
			return OpWrite, outInterface, outString, nil
		}
	}
	if Index == UpperBound+1 {
		return OpNext, nil, "", nil
	}
	return OpJump, nil, "", nil
}

func Advance(lowerBound, upperBound int) {
	LowerBound = lowerBound
	UpperBound = upperBound
}
