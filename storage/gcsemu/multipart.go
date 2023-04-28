package gcsemu

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httputil"

	"google.golang.org/api/storage/v1"
)

func readMultipartInsert(r *http.Request) (*storage.Object, []byte, error) {
	v := r.Header.Get("Content-Type")
	if v == "" {
		return nil, nil, fmt.Errorf("failed to parse Content-Type header: %q", v)
	}
	d, params, err := mime.ParseMediaType(v)
	if err != nil || d != "multipart/related" {
		return nil, nil, fmt.Errorf("failed to parse Content-Type header: %q", v)
	}
	boundary, ok := params["boundary"]
	if !ok {
		return nil, nil, fmt.Errorf("Content-Type header is missing boundary: %q", v)
	}

	reader := multipart.NewReader(r.Body, boundary)

	readPart := func() ([]byte, error) {
		part, err := reader.NextPart()
		if err != nil {
			return nil, fmt.Errorf("failed to get multipart: %w", err)
		}

		b, err := ioutil.ReadAll(part)
		if err != nil {
			return nil, fmt.Errorf("failed to get read multipart: %w", err)
		}

		return b, nil
	}

	// read the first part to get the storage.Object (in json)
	b, err := readPart()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read first part of body: %w", err)
	}

	var obj storage.Object
	err = json.Unmarshal(b, &obj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse body as json: %w", err)
	}

	// read the next part to get the file contents
	contents, err := readPart()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read second part of body: %w", err)
	}

	obj.Size = uint64(len(contents))

	return &obj, contents, nil
}

func writeMultipartResponse(r *http.Response, w io.Writer, contentId string) {
	dump, err := httputil.DumpResponse(r, true)
	if err != nil {
		fmt.Fprintf(w, "Content-Type: text/plain; charset=utf-8\r\nContent-ID: %s\r\nContent-Length: 0\r\n\r\nHTTP/1.1 500 Internal Server Error", contentId)
		return
	}
	w.Write(dump)
}
