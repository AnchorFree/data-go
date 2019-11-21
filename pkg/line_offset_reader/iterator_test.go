package line_offset_reader

import (
	eaor "github.com/anchorfree/data-go/pkg/error_at_offset_reader"
	"github.com/anchorfree/data-go/pkg/testutils"
	"github.com/anchorfree/data-go/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type lineOffsetTest struct {
	name string
	raw  string
}

var lineOffsetTests = []lineOffsetTest{
	{
		"trailing double newline",
		"first line\n" +
			"\n" +
			"\n",
	},
	{
		"multiline",
		"1234567890\r\n" +
			"1234567\n" +
			"123\n",
	},
	{
		"no_trailing_newline",
		"this is a test\r\n" +
			" that goes many lines\n" +
			"and has no newline in the end",
	},
	{
		"unicode",
		"this is a unicode symbol: Ƴ\n" +
			"and here's one more Ë to test\n",
	},
	{
		"onliner",
		"1234567890\n",
	},
	{
		"rn onliner",
		"1234567890\r\n",
	},
	{
		"onliner no new line",
		"1234567890",
	},
	{
		"emptyline",
		"",
	},
	{
		"just newline",
		"\n",
	},
	{
		"newline in between",
		"first line\n" +
			"\n" +
			"trailing line\n",
	},
	{
		"a few newlines",
		"first line\n" +
			"\n" +
			"\n" +
			"\n" +
			"\r\n" +
			"\n" +
			"trailing line\n",
	},
	{
		"long strings",
		testutils.RandomString(2096) + "\n" +
			testutils.RandomString(4099) + "\n" +
			testutils.RandomString(11000) + "\n" +
			testutils.RandomString(20000) + "\n" +
			"bottom\n",
	},
}

func TestLineSplittingAndOffsets(t *testing.T) {
	for test_index, test := range lineOffsetTests {
		inp := strings.NewReader(test.raw)
		lor := NewIterator(inp, "")
		n := 0
		offsets := testutils.GetLineOffsets(t, test.raw)
		lengths := testutils.GetLineLengths(t, test.raw)
		for lor.Next() {
			event := lor.At()
			require.Falsef(t, n+1 > len(offsets), "Found more lines that expected in test %d \"%s\" (%d vs %d)", test_index, test.name, n+1, len(offsets))
			assert.Falsef(t, event.Offset != offsets[n], "Line offset doesn't match in test #%d \"%s\" line #%d (%d vs %d)", test_index, test.name, n, event.Offset, offsets[n])
			assert.Falsef(t, len(event.Message) != lengths[n], "Line length doesn't match in test #%d \"%s\" line #%d (%d vs %d)", test_index, test.name, n, len(event.Message), lengths[n])
			n++
		}
		assert.Falsef(t, n != testutils.GetLineCount(t, test.raw), "Line count doesn't match in test #%d \"%s\" (%d vs %d)", test_index, test.name, n, testutils.GetLineCount(t, test.raw))
	}
}

func TestInterruptedLineReader(t *testing.T) {
	for test_index, test := range lineOffsetTests {
		var (
			errorAtOffset int          = 2
			event         *types.Event = &types.Event{}
		)
		stringReader := strings.NewReader(test.raw)
		inp := eaor.NewErrorAtOffsetReader(stringReader, errorAtOffset)
		lor := NewIterator(inp, "")
		n := 0
		for lor.Next() {
			//line, offset, err := lor.ReadLine()
			event = lor.At()
			n++
		}
		assert.Falsef(t, uint64(errorAtOffset) < event.Offset && (lor.Err() == nil),
			"Error reader didn't trigger an error in test #%d \"%s\" (%d vs %d)", test_index, test.name, n, testutils.GetLineCount(t, test.raw))
	}
}

type jsonMessageTest struct {
	name           string
	raw            string
	offsets        []uint64
	lengths        []int
	trimmedLengths []int
}

var jsonMessageTests = []jsonMessageTest{
	{
		"two jsons",
		`{"event":"test"}{"event":"extra"}`,
		[]uint64{0, 16},
		[]int{16, 17},
		[]int{16, 17},
	},
	{
		"two jsons with whitespaces",
		"\"{event\":\"test\"} \t{\"event\":\"1234\"} s\n",
		[]uint64{0, 18, 35},
		[]int{18, 17, 1},
		[]int{16, 16, 1},
	},
}

func TestReadJsonMessageAndOffsets(t *testing.T) {
	for ind, test := range jsonMessageTests {
		inp := strings.NewReader(test.raw)
		lor := NewIterator(inp, "")
		lor.LookForJsonDelimiters(true)
		n := 0
		for lor.Next() {
			require.Falsef(t, n+1 > len(test.offsets), "Found more lines that expected in test %d \"%s\" (%d vs %d)", ind, test.name, n+1, len(test.offsets))
			event := lor.At()
			assert.Equalf(t, test.offsets[n], event.Offset, "Line offset doesn't match in test #%d \"%s\" line #%d (%d vs %d)", ind, test.name, n, test.offsets[n], event.Offset)
			assert.Equalf(t, test.lengths[n], len(event.Message), "Line length doesn't match in test #%d \"%s\" line #%d (%d vs %d)", ind, test.name, n, test.lengths[n], len(event.Message))
			n++
		}
		assert.Equalf(t, len(test.offsets), n, "Line count doesn't match in test #%d \"%s\" (%d vs %d)", ind, test.name, len(test.offsets), n)
	}
}

func TestReadJsonMessageTrimmedAndOffsets(t *testing.T) {
	for ind, test := range jsonMessageTests {
		inp := strings.NewReader(test.raw)
		lor := NewIterator(inp, "")
		lor.LookForJsonDelimiters(true)
		lor.TrimMessages(true)
		n := 0
		for lor.Next() {
			require.Falsef(t, n+1 > len(test.offsets), "Found more lines that expected in test %d \"%s\" (%d vs %d)", ind, test.name, n+1, len(test.offsets))
			event := lor.At()
			assert.Equalf(t, test.offsets[n], event.Offset, "Line offset doesn't match in test #%d \"%s\" line #%d (%d vs %d)", ind, test.name, n, test.offsets[n], event.Offset)
			assert.Equalf(t, test.trimmedLengths[n], len(event.Message), "Line length doesn't match in test #%d \"%s\" line #%d (%d vs %d)", ind, test.name, n, test.trimmedLengths[n], len(event.Message))
			n++
		}
		assert.Equalf(t, len(test.offsets), n, "Line count doesn't match in test #%d \"%s\" (%d vs %d)", ind, test.name, len(test.offsets), n)
	}
}
