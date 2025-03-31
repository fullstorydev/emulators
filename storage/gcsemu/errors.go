package gcsemu

import (
	"errors"
	"fmt"

	"google.golang.org/api/googleapi"
)

func httpStatusCodeOf(err error) int {
	var gapiErr *googleapi.Error
	if errors.As(err, &gapiErr) {
		return gapiErr.Code
	}

	var httpErr *httpError
	if errors.As(err, &httpErr) {
		if httpErr.code != 0 {
			return httpErr.code
		}
		return httpStatusCodeOf(httpErr.cause)
	}
	return 0
}

func fmtErrorfCode(httpCode int, f string, args ...interface{}) error {
	return &httpError{
		cause: fmt.Errorf(f, args...),
		code:  httpCode,
	}
}

// httpError is a custom error type that decorates with an HTTP error code
type httpError struct {
	cause error
	code  int
}

// Error returns a string describing the entire causal chain.
func (err *httpError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("http error %d: %s", err.code, err.cause)
}

func (err *httpError) Unwrap() error {
	return err.cause
}
