package gcsemu

import (
	"fmt"
	"mime"
	"strings"

	"google.golang.org/api/storage/v1"
)

func bucketMeta(baseUrl httpBaseUrl, bucket string) *storage.Bucket {
	return &storage.Bucket{
		Kind:         "storage#bucket",
		Name:         bucket,
		SelfLink:     bucketUrl(baseUrl, bucket),
		StorageClass: "STANDARD",
	}
}

// initScrubbedMeta "bakes" metadata with intrinsic values and removes fields that are intrinsic / computed.
func initScrubbedMeta(meta *storage.Object, filename string) {
	parts := strings.Split(filename, ".")
	ext := parts[len(parts)-1]

	if meta.ContentType == "" {
		meta.ContentType = mime.TypeByExtension(ext)
	}
	meta.Name = filename
	scrubMeta(meta)
}

// initMetaWithUrls "bakes" metadata with intrinsic values, including computed links.
func initMetaWithUrls(baseUrl httpBaseUrl, meta *storage.Object, bucket string, filename string, size uint64) {
	parts := strings.Split(filename, ".")
	ext := parts[len(parts)-1]

	meta.Bucket = bucket
	if meta.ContentType == "" {
		meta.ContentType = mime.TypeByExtension(ext)
	}
	meta.Kind = "storage#object"
	meta.MediaLink = objectUrl(baseUrl, bucket, filename) + "?alt=media"
	meta.Name = filename
	meta.SelfLink = objectUrl(baseUrl, bucket, filename)
	meta.Size = size
	meta.StorageClass = "STANDARD"
}

// scrubMeta removes fields that are intrinsic / computed for minimal storage.
func scrubMeta(meta *storage.Object) {
	meta.Bucket = ""
	meta.Kind = ""
	meta.MediaLink = ""
	meta.SelfLink = ""
	meta.Size = 0
	meta.StorageClass = ""
}

// Return the URL for a bucket.
func bucketUrl(baseUrl httpBaseUrl, bucket string) string {
	return fmt.Sprintf("%sstorage/v1/b/%s", normalizeBaseUrl(baseUrl), bucket)
}

// Return the URL for a file.
func objectUrl(baseUrl httpBaseUrl, bucket string, filepath string) string {
	return fmt.Sprintf("%sstorage/v1/b/%s/o/%s", normalizeBaseUrl(baseUrl), bucket, filepath)
}

// emulator base URL, including trailing slash; e.g. https://www.googleapis.com/
type httpBaseUrl string

// when the caller doesn't really care about the object meta URLs
const dontNeedUrls = httpBaseUrl("")

func normalizeBaseUrl(baseUrl httpBaseUrl) httpBaseUrl {
	if baseUrl == dontNeedUrls || baseUrl == "https://storage.googleapis.com/" {
		return "https://www.googleapis.com/"
	} else if baseUrl == "http://storage.googleapis.com/" {
		return "http://www.googleapis.com/"
	} else {
		return baseUrl
	}
}
