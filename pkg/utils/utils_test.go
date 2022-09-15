package utils

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestUniqueStringSlice(t *testing.T) {
	tests := []struct {
		Data   []string
		Result []string
	}{
		{
			[]string{"one", "two", "three", "three"},
			[]string{"one", "two", "three"},
		},
		{
			[]string{"one", "two", "три\u1234", "три\u1234"},
			[]string{"one", "two", "три\u1234"},
		},
		{
			[]string{"one", "two", "3 3", "3 3"},
			[]string{"one", "two", "3 3"},
		},
	}
	for _, test := range tests {
		assert.Equal(t, test.Result, UniqueStringSlice(test.Data))
	}
}

var sanityzeCoordinatesTests = []struct {
	in  string
	out string
}{
	{".23", ".23"}, // require integer part
	{"1.23,.56", "1.x,.56"},
	{".56,1.23", ".56,1.x"},
	{"123.456", "123.x"},
	{"123.456,789.012", "123.x,789.x"},
	{"123.456,789.012,345.678", "123.x,789.x,345.x"},
	{"123.456, 789.012", "123.x, 789.x"},
	{"123.456, 789.012, 345.678", "123.x, 789.x, 345.x"},
}

func TestSanityzeCoordinates(t *testing.T) {
	for _, tt := range sanityzeCoordinatesTests {
		if s := SanityzeCoordinates(tt.in); s != tt.out {
			t.Errorf("SanityzeCoordinates for %q = %q, want %q", tt.in, s, tt.out)
		}
	}
}

func TestSanityzeForLogs(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{"", ""},
		{"abc", "abc"},
		{"abc\r", "abc"},
		{"abc\n", "abc"},
		{"abc\rdef", "abcdef"},
		{"abc\ndef", "abcdef"},
		{"abc\rdef\r", "abcdef"},
		{"abc\ndef\n", "abcdef"},
		{"abc\r\ndef\r", "abcdef"},
		{"abc\r\ndef\n", "abcdef"},
	}
	for _, tt := range tests {
		if s := SanityzeForLogs(tt.in); s != tt.out {
			t.Errorf("sanityzeForLogs for %q = %q, want %q", tt.in, s, tt.out)
		}
	}
}

var getHeaderListTests = []struct {
	s string
	l []string
}{
	{s: `a`, l: []string{`a`}},
	{s: `a, b , c `, l: []string{`a`, `b`, `c`}},
	{s: `a,, b , , c `, l: []string{`a`, `b`, `c`}},
	{s: `a,b,c`, l: []string{`a`, `b`, `c`}},
	{s: ` a b, c d `, l: []string{`a b`, `c d`}},
	{s: `"a, b, c", d `, l: []string{`"a, b, c"`, "d"}},
	{s: `","`, l: []string{`","`}},
	{s: `"\""`, l: []string{`"\""`}},
	{s: `" "`, l: []string{`" "`}},
}

func TestGetHeaderList(t *testing.T) {
	for _, tt := range getHeaderListTests {
		header := http.Header{"Foo": {tt.s}}
		if l := ParseList(header, "foo"); !cmp.Equal(tt.l, l) {
			t.Errorf("ParseList for %q = %q, want %q", tt.s, l, tt.l)
		}
	}
}
