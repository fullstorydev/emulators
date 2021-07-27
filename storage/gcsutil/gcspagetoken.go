package gcsutil

import (
	"encoding/base64"
	"fmt"

	"github.com/golang/protobuf/proto"
)

// The internal format of a GCS PageToken cursor.
type gcsPageToken struct {
	// The full name of the last result file, when returned from the server.
	// When sent as a cursor, interpreted as "return files greater than this value".
	LastFile string `protobuf:"bytes,1,opt,name=lastFile" json:"lastFile,omitempty"`
}

func (m *gcsPageToken) Reset()         { *m = gcsPageToken{} }
func (m *gcsPageToken) String() string { return proto.CompactTextString(m) }
func (*gcsPageToken) ProtoMessage()    {}

// EncodePageToken returns a synthetic page token to find files greater than the given string.
// If this is part of a prefix query, the token should fall within the prefixed range.
// BRITTLE: relies on a reverse-engineered internal GCS token format, which may be subject to change.
func EncodePageToken(greaterThan string) string {
	bytes, err := proto.Marshal(&gcsPageToken{
		LastFile: greaterThan,
	})
	if err != nil {
		panic("could not encode gcsPageToken:" + err.Error())
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

// DecodePageToken decodes a GCS pageToken to the name of the last file returned.
func DecodePageToken(pageToken string) (string, error) {
	bytes, err := base64.StdEncoding.DecodeString(pageToken)
	if err != nil {
		return "", fmt.Errorf("could not base64 decode pageToken %s: %w", pageToken, err)
	}
	var message gcsPageToken
	if err := proto.Unmarshal(bytes, &message); err != nil {
		return "", fmt.Errorf("could not unmarshal proto: %w", err)
	}

	return message.LastFile, nil
}
