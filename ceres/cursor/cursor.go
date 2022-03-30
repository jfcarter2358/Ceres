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

func Next(line string, datum interface{}) (IOOp, map[string]interface{}, string, error) {
	Index += 1
	if Index >= LowerBound && Index <= UpperBound {
		if Mode == ModeRead {
			var outInterface map[string]interface{}
			err := json.Unmarshal([]byte(line), &outInterface)
			if err != nil {
				return OpError, nil, "", err
			}
			return OpRead, outInterface, "", nil
		} else if Mode == ModeWrite {
			var outString string
			outBytes, err := json.Marshal(datum)
			if err != nil {
				return OpError, nil, "", err
			}
			outString = string(outBytes) + "\n"
			return OpWrite, nil, outString, nil
		} else if Mode == ModeDelete {
			var outString string
			outString = "\n"
			return OpDelete, nil, outString, nil
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
