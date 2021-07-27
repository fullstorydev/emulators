package gcsclient

import (
	"context"
	"net/http"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// NewTestClientWithHost returns a new Google storage client that connects to the given host:port address.
func NewTestClientWithHost(ctx context.Context, addr string) (*storage.Client, error) {
	delegate := http.DefaultTransport
	httpClient := &http.Client{
		Transport: tripperFunc(func(r *http.Request) (*http.Response, error) {
			r = r.Clone(r.Context())
			r.URL.Host = addr
			r.URL.Scheme = "http"
			return delegate.RoundTrip(r)
		}),
	}
	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

type tripperFunc func(*http.Request) (*http.Response, error)

func (f tripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
