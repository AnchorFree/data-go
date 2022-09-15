package utils

import (
	"net/http"
	"regexp"
	"strings"
)

func UniqueStringSlice(inp []string) []string {
	ret := []string{}
	for _, candidate := range inp {
		duplicate := false
		for _, member := range ret {
			if candidate == member {
				duplicate = true
				break
			}
		}
		if !duplicate {
			ret = append(ret, candidate)
		}
	}
	return ret
}

// replace all digits after decimal dot with 'x'
// this will obfuscate location to "country or large region"
// 1.23456789 -> 1.x
func SanityzeCoordinates(src string) string {
	escapedSrc := strings.Replace(src, "\n", "", -1)
	escapedSrc = strings.Replace(escapedSrc, "\r", "", -1)
	rgx := regexp.MustCompile(`([0-9]+)\.[0-9]+`)
	return rgx.ReplaceAllString(escapedSrc, "$1.x")
}

func SanityzeForLogs(src string) string {
	escapedSrc := strings.Replace(src, "\n", "", -1)
	escapedSrc = strings.Replace(escapedSrc, "\r", "", -1)
	return escapedSrc
}

// proudly stolen from
// https://github.com/golang/gddo/blob/20d68f94ee1f7547de2b1c68627253df20c8d45e/httputil/header/header.go#L86

// Octet types from RFC 2616.
var octetTypes [256]octetType

type octetType byte

const (
	isToken octetType = 1 << iota
	isSpace
)

func init() {
	// OCTET      = <any 8-bit sequence of data>
	// CHAR       = <any US-ASCII character (octets 0 - 127)>
	// CTL        = <any US-ASCII control character (octets 0 - 31) and DEL (127)>
	// CR         = <US-ASCII CR, carriage return (13)>
	// LF         = <US-ASCII LF, linefeed (10)>
	// SP         = <US-ASCII SP, space (32)>
	// HT         = <US-ASCII HT, horizontal-tab (9)>
	// <">        = <US-ASCII double-quote mark (34)>
	// CRLF       = CR LF
	// LWS        = [CRLF] 1*( SP | HT )
	// TEXT       = <any OCTET except CTLs, but including LWS>
	// separators = "(" | ")" | "<" | ">" | "@" | "," | ";" | ":" | "\" | <">
	//              | "/" | "[" | "]" | "?" | "=" | "{" | "}" | SP | HT
	// token      = 1*<any CHAR except CTLs or separators>
	// qdtext     = <any TEXT except <">>

	// nolint: gosimple
	for c := 0; c < 256; c++ {
		var t octetType
		isCtl := c <= 31 || c == 127
		isChar := 0 <= c && c <= 127
		isSeparator := strings.IndexRune(" \t\"(),/:;<=>?@[]\\{}", rune(c)) >= 0
		if strings.IndexRune(" \t\r\n", rune(c)) >= 0 {
			t |= isSpace
		}
		if isChar && !isCtl && !isSeparator {
			t |= isToken
		}
		octetTypes[c] = t
	}
}

// ParseList parses a comma separated list of values. Commas are ignored in
// quoted strings. Quoted values are not unescaped or unquoted. Whitespace is trimmed.
func ParseList(header http.Header, key string) []string {
	var result []string
	for _, s := range header[http.CanonicalHeaderKey(key)] {
		begin := 0
		end := 0
		escape := false
		quote := false
		for i := 0; i < len(s); i++ {
			b := s[i]
			switch {
			case escape:
				escape = false
				end = i + 1
			case quote:
				switch b {
				case '\\':
					escape = true
				case '"':
					quote = false
				}
				end = i + 1
			case b == '"':
				quote = true
				end = i + 1
			case octetTypes[b]&isSpace != 0:
				if begin == end {
					begin = i + 1
					end = begin
				}
			case b == ',':
				if begin < end {
					result = append(result, s[begin:end])
				}
				begin = i + 1
				end = begin
			default:
				end = i + 1
			}
		}
		if begin < end {
			result = append(result, s[begin:end])
		}
	}
	return result
}
