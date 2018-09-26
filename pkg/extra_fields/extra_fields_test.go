package extra_fields

import (
	"github.com/stretchr/testify/assert"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestGetIPAddress(t *testing.T) {
	// Create type and function for testing
	type testIP struct {
		name     string
		request  *http.Request
		expected string
	}

	newRequest := func(remoteAddr, xRealIP string, xForwardedFor ...string) *http.Request {
		h := http.Header{}
		h.Set("X-Real-IP", xRealIP)
		if len(xForwardedFor) > 0 {
			h.Set("X-Forwarded-For", strings.Join(xForwardedFor, ","))
		}

		return &http.Request{
			RemoteAddr: remoteAddr,
			Header:     h,
		}
	}

	// Create test data
	publicAddr1 := "144.12.54.87"
	publicAddr2 := "119.14.55.11"
	publicAddr3 := "74.115.4.68"
	localAddr := "127.0.0.0"

	testData := []testIP{
		{
			name:     "No header",
			request:  newRequest(publicAddr1, ""),
			expected: publicAddr1,
		}, {
			name:     "Has X-Forwarded-For",
			request:  newRequest("", "", publicAddr1),
			expected: publicAddr1,
		}, {
			name:     "Has multiple X-Forwarded-For",
			request:  newRequest("", "", localAddr, publicAddr1, publicAddr2),
			expected: publicAddr1,
		}, {
			name:     "Multiple X-Forwarded-For coma separated values",
			request:  newRequest("74.115.4.68", "", localAddr, publicAddr1, publicAddr2, publicAddr3),
			expected: publicAddr1,
		}, {
			name:     "Has X-Real-IP",
			request:  newRequest("", publicAddr1),
			expected: publicAddr1,
		},
	}

	// Run test
	for _, v := range testData {
		ip := GetIPAdress(v.request)
		assert.Equalf(t, net.ParseIP(v.expected).String(), ip.String(), "test: %s - ip does not match", v.name)
	}
}
