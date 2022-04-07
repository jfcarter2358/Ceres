// utils.go

package utils

import (
	"bufio"
	"sort"
)

func BuildRangeBlocks(indices []int) [][]int {
	var output [][]int
	sort.Ints(indices)

	beginIdx := indices[0]
	endIdx := indices[0]

	for _, idx := range indices[1:] {
		if idx == endIdx || idx == endIdx+1 {
			// If there is not gap then widen the block
			endIdx = idx
		} else {
			// If there is a gap then record the block
			newBlock := []int{beginIdx, endIdx}
			output = append(output, newBlock)
			beginIdx = idx
			endIdx = idx
		}
	}

	// Make sure we include the last block as well
	newBlock := []int{beginIdx, endIdx}
	output = append(output, newBlock)

	return output
}

func CombineRangeBlocks(indices_left, indices_right [][]int) [][]int {
	combined := append(indices_left, indices_right...)
	BlockSort(combined)

	output := make([][]int, 0)
	left := combined[0][0]
	right := combined[0][1]

	for _, block := range combined[1:] {
		if block[0] <= right {
			right = block[1]
			continue
		}
		newBlock := []int{left, right}
		output = append(output, newBlock)
		left = block[0]
		right = block[1]
	}
	newBlock := []int{left, right}
	output = append(output, newBlock)

	return output
}

func BlockSort(arr [][]int) {
	sort.Slice(arr, func(i, j int) bool {
		if arr[i][0] < arr[j][0] {
			return true
		}
		if arr[i][0] == arr[j][0] && arr[i][1] < arr[j][1] {
			return true
		}
		return false
	})
}

func ReadLine(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
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
