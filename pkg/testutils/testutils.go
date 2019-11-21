package testutils

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"testing"
)

var letters = []rune(" \t,abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TrimNewLine(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\n' {
		s = s[:len(s)-1]
	}
	if len(s) > 0 && s[len(s)-1] == '\r' {
		s = s[:len(s)-1]
	}
	return s
}

func GetLineCount(t *testing.T, s string) int {
	s = TrimNewLine(s)
	lines := SplitString(s)
	return len(lines)
}

func SplitString(s string) []string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, "\r")
	}
	return lines
}

func GetLineLengths(t *testing.T, s string) []int {
	t.Helper()
	s = TrimNewLine(s)
	lines := SplitString(s)
	var lengths []int
	for _, line := range lines {
		lengths = append(lengths, len(line))
	}
	return lengths
}

func GetLineOffsets(t *testing.T, s string) []uint64 {
	t.Helper()
	var (
		offsets []uint64
	)
	s = TrimNewLine(s)
	//first line is always 0 offset
	offsets = append(offsets, uint64(0))
	for pos, runeChar := range s {
		char := string(runeChar)
		if char == "\n" {
			//assuming we trimmed \n at the end of line,
			//so a new line can not be the last char in the string
			//and whatever goes next should be a position of new line
			assert.Falsef(t, pos+1 > len(s), "Error. Estimated position of next line is bigger than the sring size (%d vs %d)\n", pos+1, len(s))
			offsets = append(offsets, uint64(pos+1))
		}
	}
	if len(SplitString(s)) != len(offsets) {
		t.Fatalf("Error. HelperGetLineOffsets and SplitString counts mismatch (%d vs %d)\n", len(SplitString(s)), len(offsets))
	}
	return offsets
}
