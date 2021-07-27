package gcsemu

import (
	"fmt"
	"mime"
	"strings"

	"google.golang.org/api/storage/v1"
)

func bucketMeta(bucket string) *storage.Bucket {
	return &storage.Bucket{
		Kind:         "storage#bucket",
		Name:         bucket,
		SelfLink:     bucketUrl(bucket),
		StorageClass: "STANDARD",
	}
}

// initMeta "bakes" metadata with intrinsic values.
func initMeta(meta *storage.Object, bucket string, filename string, size uint64) {
	parts := strings.Split(filename, ".")
	ext := parts[len(parts)-1]

	meta.Bucket = bucket
	if meta.ContentType == "" {
		meta.ContentType = mime.TypeByExtension(ext)
	}
	meta.Kind = "storage#object"
	meta.MediaLink = objectUrl(bucket, filename) + "?alt=media"
	meta.Name = filename
	meta.SelfLink = objectUrl(bucket, filename)
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

// Return the URL for a file.  Note that this will generally not be useful as an input an http request due to the way
// that net/url does character substitution (see comments above).
func bucketUrl(bucket string) string {
	return fmt.Sprintf("https://www.googleapis.com/storage/v1/b/%s", bucket)
}

// Return the URL for a file.
func objectUrl(bucket string, filepath string) string {
	return fmt.Sprintf("https://www.googleapis.com/storage/v1/b/%s/o/%s", bucket, filepath)
}
