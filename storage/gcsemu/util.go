package gcsemu

import (
	"encoding/json"
	"errors"
	"net/http"

	"google.golang.org/api/googleapi"
)

// jsonRespond json-encodes rsp and writes it to w.  If an error occurs, then it is logged and a 500 error is written to w.
func (g *GcsEmu) jsonRespond(w http.ResponseWriter, rsp interface{}) {
	// do NOT write a http status since OK will be the default and this allows the caller to use their own if they want
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	encoder := json.NewEncoder(w)
	if err := encoder.Encode(rsp); err != nil {
		g.log(err, "failed to send response")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

// gapiError responds to the client with a GAPI error
func (g *GcsEmu) gapiError(w http.ResponseWriter, code int, message string) {
	if code == 0 {
		code = http.StatusInternalServerError
	}
	if code != http.StatusNotFound {
		g.log(errors.New(message), "responding with HTTP %d", code)
	}
	if message == "" {
		message = http.StatusText(code)
	}

	// format copied from errorReply struct in google.golang.org/api/googleapi
	rsp := struct {
		Error *googleapi.Error `json:"error"`
	}{
		Error: &googleapi.Error{
			Code:    code,
			Message: message,
		},
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(&rsp)
}

// mustJson serializes the given value to json, panicking on failure
func mustJson(val interface{}) []byte {
	if val == nil {
		return []byte("null")
	}

	b, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		panic(err)
	}
	return b
}
