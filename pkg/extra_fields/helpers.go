package extra_fields

import (
	"bytes"
)

func AppendJsonExtraFields(line []byte, extra []byte) []byte {
	if len(line) == 0 {
		return extra
	}
	// we don't need to merge empty extra fileds
	if 0 == len(extra) || bytes.Equal(extra, []byte("{}")) {
		return line
	}
	// could not find closing breaket, broken json
	if bytes.LastIndex(line, []byte("}")) == -1 {
		return line
	}
	return bytes.Join([][]byte{(line)[:bytes.LastIndex(line, []byte("}"))], extra[1:]}, []byte(","))
}
