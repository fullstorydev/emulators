package gcsutil

import (
	"google.golang.org/api/googleapi"
	"google.golang.org/api/storage/v1"
)

//Changes made:
//1) made Size in Object pointer so omitempty wont remove 0
//2) made ObjectSize in RewriteResponse pointer so omitempty wont remove 0
//3) made TotalBytesRewritten in RewriteResponse pointer so omitempty wont remove 0

// Object: An object.
type Object struct {
	// Acl: Access controls on the object.
	Acl []*storage.ObjectAccessControl `json:"acl,omitempty"`

	// Bucket: The name of the bucket containing this object.
	Bucket string `json:"bucket,omitempty"`

	// CacheControl: Cache-Control directive for the object data. If
	// omitted, and the object is accessible to all anonymous users, the
	// default will be public, max-age=3600.
	CacheControl string `json:"cacheControl,omitempty"`

	// ComponentCount: Number of underlying components that make up this
	// object. Components are accumulated by compose operations.
	ComponentCount int64 `json:"componentCount,omitempty"`

	// ContentDisposition: Content-Disposition of the object data.
	ContentDisposition string `json:"contentDisposition,omitempty"`

	// ContentEncoding: Content-Encoding of the object data.
	ContentEncoding string `json:"contentEncoding,omitempty"`

	// ContentLanguage: Content-Language of the object data.
	ContentLanguage string `json:"contentLanguage,omitempty"`

	// ContentType: Content-Type of the object data. If an object is stored
	// without a Content-Type, it is served as application/octet-stream.
	ContentType string `json:"contentType,omitempty"`

	// Crc32c: CRC32c checksum, as described in RFC 4960, Appendix B;
	// encoded using base64 in big-endian byte order. For more information
	// about using the CRC32c checksum, see Hashes and ETags: Best
	// Practices.
	Crc32c string `json:"crc32c,omitempty"`

	// CustomTime: A timestamp in RFC 3339 format specified by the user for
	// an object.
	CustomTime string `json:"customTime,omitempty"`

	// CustomerEncryption: Metadata of customer-supplied encryption key, if
	// the object is encrypted by such a key.
	CustomerEncryption *storage.ObjectCustomerEncryption `json:"customerEncryption,omitempty"`

	// Etag: HTTP 1.1 Entity tag for the object.
	Etag string `json:"etag,omitempty"`

	// EventBasedHold: Whether an object is under event-based hold.
	// Event-based hold is a way to retain objects until an event occurs,
	// which is signified by the hold's release (i.e. this value is set to
	// false). After being released (set to false), such objects will be
	// subject to bucket-level retention (if any). One sample use case of
	// this flag is for banks to hold loan documents for at least 3 years
	// after loan is paid in full. Here, bucket-level retention is 3 years
	// and the event is the loan being paid in full. In this example, these
	// objects will be held intact for any number of years until the event
	// has occurred (event-based hold on the object is released) and then 3
	// more years after that. That means retention duration of the objects
	// begins from the moment event-based hold transitioned from true to
	// false.
	EventBasedHold bool `json:"eventBasedHold,omitempty"`

	// Generation: The content generation of this object. Used for object
	// versioning.
	Generation int64 `json:"generation,omitempty,string"`

	// Id: The ID of the object, including the bucket name, object name, and
	// generation number.
	Id string `json:"id,omitempty"`

	// Kind: The kind of item this is. For objects, this is always
	// storage#object.
	Kind string `json:"kind,omitempty"`

	// KmsKeyName: Not currently supported. Specifying the parameter causes
	// the request to fail with status code 400 - Bad Request.
	KmsKeyName string `json:"kmsKeyName,omitempty"`

	// Md5Hash: MD5 hash of the data; encoded using base64. For more
	// information about using the MD5 hash, see Hashes and ETags: Best
	// Practices.
	Md5Hash string `json:"md5Hash,omitempty"`

	// MediaLink: Media download link.
	MediaLink string `json:"mediaLink,omitempty"`

	// Metadata: User-provided metadata, in key/value pairs.
	Metadata map[string]string `json:"metadata,omitempty"`

	// Metageneration: The version of the metadata for this object at this
	// generation. Used for preconditions and for detecting changes in
	// metadata. A metageneration number is only meaningful in the context
	// of a particular generation of a particular object.
	Metageneration int64 `json:"metageneration,omitempty,string"`

	// Name: The name of the object. Required if not specified by URL
	// parameter.
	Name string `json:"name,omitempty"`

	// Owner: The owner of the object. This will always be the uploader of
	// the object.
	Owner *storage.ObjectOwner `json:"owner,omitempty"`

	// RetentionExpirationTime: A server-determined value that specifies the
	// earliest time that the object's retention period expires. This value
	// is in RFC 3339 format. Note 1: This field is not provided for objects
	// with an active event-based hold, since retention expiration is
	// unknown until the hold is removed. Note 2: This value can be provided
	// even when temporary hold is set (so that the user can reason about
	// policy without having to first unset the temporary hold).
	RetentionExpirationTime string `json:"retentionExpirationTime,omitempty"`

	// SelfLink: The link to this object.
	SelfLink string `json:"selfLink,omitempty"`

	// Size: Content-Length of the data in bytes.
	//Change size to pointer so that it is not omitted if it is zero which is a bug
	Size *uint64 `json:"size,omitempty,string"`

	// StorageClass: Storage class of the object.
	StorageClass string `json:"storageClass,omitempty"`

	// TemporaryHold: Whether an object is under temporary hold. While this
	// flag is set to true, the object is protected against deletion and
	// overwrites. A common use case of this flag is regulatory
	// investigations where objects need to be retained while the
	// investigation is ongoing. Note that unlike event-based hold,
	// temporary hold does not impact retention expiration time of an
	// object.
	TemporaryHold bool `json:"temporaryHold,omitempty"`

	// TimeCreated: The creation time of the object in RFC 3339 format.
	TimeCreated string `json:"timeCreated,omitempty"`

	// TimeDeleted: The deletion time of the object in RFC 3339 format. Will
	// be returned if and only if this version of the object has been
	// deleted.
	TimeDeleted string `json:"timeDeleted,omitempty"`

	// TimeStorageClassUpdated: The time at which the object's storage class
	// was last changed. When the object is initially created, it will be
	// set to timeCreated.
	TimeStorageClassUpdated string `json:"timeStorageClassUpdated,omitempty"`

	// Updated: The modification time of the object metadata in RFC 3339
	// format. Set initially to object creation time and then updated
	// whenever any metadata of the object changes. This includes changes
	// made by a requester, such as modifying custom metadata, as well as
	// changes made by Cloud Storage on behalf of a requester, such as
	// changing the storage class based on an Object Lifecycle
	// Configuration.
	Updated string `json:"updated,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Acl") to
	// unconditionally include in API requests. By default, fields with
	// empty or default values are omitted from API requests. However, any
	// non-pointer, non-interface field appearing in ForceSendFields will be
	// sent to the server regardless of whether the field is empty or not.
	// This may be used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Acl") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

// Objects: A list of objects.
type Objects struct {
	// Items: The list of items.
	Items []*Object `json:"items,omitempty"`

	// Kind: The kind of item this is. For lists of objects, this is always
	// storage#objects.
	Kind string `json:"kind,omitempty"`

	// NextPageToken: The continuation token, used to page through large
	// result sets. Provide this value in a subsequent request to return the
	// next page of results.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// Prefixes: The list of prefixes of objects matching-but-not-listed up
	// to and including the requested delimiter.
	Prefixes []string `json:"prefixes,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Items") to
	// unconditionally include in API requests. By default, fields with
	// empty or default values are omitted from API requests. However, any
	// non-pointer, non-interface field appearing in ForceSendFields will be
	// sent to the server regardless of whether the field is empty or not.
	// This may be used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Items") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

// RewriteResponse: A rewrite response.
type RewriteResponse struct {
	// Done: true if the copy is finished; otherwise, false if the copy is
	// in progress. This property is always present in the response.
	Done bool `json:"done,omitempty"`

	// Kind: The kind of item this is.
	Kind string `json:"kind,omitempty"`

	// ObjectSize: The total size of the object being copied in bytes. This
	// property is always present in the response.
	ObjectSize *int64 `json:"objectSize,omitempty,string"`

	// Resource: A resource containing the metadata for the copied-to
	// object. This property is present in the response only when copying
	// completes.
	Resource *Object `json:"resource,omitempty"`

	// RewriteToken: A token to use in subsequent requests to continue
	// copying data. This token is present in the response only when there
	// is more data to copy.
	RewriteToken string `json:"rewriteToken,omitempty"`

	// TotalBytesRewritten: The total bytes written so far, which can be
	// used to provide a waiting user with a progress indicator. This
	// property is always present in the response.
	TotalBytesRewritten *int64 `json:"totalBytesRewritten,omitempty,string"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Done") to
	// unconditionally include in API requests. By default, fields with
	// empty or default values are omitted from API requests. However, any
	// non-pointer, non-interface field appearing in ForceSendFields will be
	// sent to the server regardless of whether the field is empty or not.
	// This may be used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Done") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

// ComposeRequest: A Compose request.
type ComposeRequest struct {
	// Destination: Properties of the resulting object.
	Destination *Object `json:"destination,omitempty"`

	// Kind: The kind of item this is.
	Kind string `json:"kind,omitempty"`

	// SourceObjects: The list of source objects that will be concatenated
	// into a single object.
	SourceObjects []*storage.ComposeRequestSourceObjects `json:"sourceObjects,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Destination") to
	// unconditionally include in API requests. By default, fields with
	// empty or default values are omitted from API requests. However, any
	// non-pointer, non-interface field appearing in ForceSendFields will be
	// sent to the server regardless of whether the field is empty or not.
	// This may be used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Destination") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}
