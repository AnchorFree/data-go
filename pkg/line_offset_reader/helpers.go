package line_offset_reader

func IsWhiteSpace(b byte) bool {
	for _, w := range []byte("\n\t \u000A") {
		if b == w {
			return true
		}
	}
	return false
}
