package logging_response_writer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggingResponseWriter(t *testing.T) {
	w := httptest.NewRecorder()
	lrw := NewLoggingResponseWriter(w)
	assert.Equal(t, w.Result().StatusCode, lrw.StatusCode(), "LoggingResponseWriter default status wrong")
	lrw.WriteHeader(http.StatusBadRequest)
	assert.Equal(t, http.StatusBadRequest, lrw.StatusCode(), "LoggingResponseWriter didn't get reset through WriteHeader")
	errorMessage := "test-error-message"
	w = httptest.NewRecorder()
	lrw = NewLoggingResponseWriter(w)
	http.Error(lrw, errorMessage, http.StatusNotFound)
	assert.Equal(t, http.StatusNotFound, lrw.StatusCode(), "LoggingResponseWriter didn't get reset through http.Error")
	responseBody, _ := io.ReadAll(w.Result().Body)
	assert.Equal(t, errorMessage, strings.Trim(string(responseBody), "\n"), "LoggingResponseWriter didn't write error message to the response body when invoked http.Error")
}
