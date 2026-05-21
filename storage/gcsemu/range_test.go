package gcsemu

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestParseByteRange(t *testing.T) {
	tcs := []struct {
		in     string
		expect byteRange
	}{
		{in: "bytes 0-8388607/*", expect: byteRange{lo: 0, hi: 8388607, sz: -1}},
		{in: "bytes 8388608-10485759/10485760", expect: byteRange{lo: 8388608, hi: 10485759, sz: 10485760}},
		{in: "bytes */10485760", expect: byteRange{lo: -1, hi: -1, sz: 10485760}},
	}

	for _, tc := range tcs {
		t.Logf("test case: %s", tc.in)
		assert.Equal(t, tc.expect, *parseByteRange(tc.in))
	}
}

func TestParseRangeRequestHeader(t *testing.T) {
	tcs := []struct {
		header    string
		totalSize int64
		lo, hi    int64
		ok        bool
	}{
		{header: "bytes=0-99", totalSize: 500, lo: 0, hi: 99, ok: true},
		{header: "bytes=0-99", totalSize: 50, lo: 0, hi: 49, ok: true},
		{header: "bytes=10-", totalSize: 500, lo: 10, hi: 499, ok: true},
		{header: "bytes=-100", totalSize: 500, lo: 400, hi: 499, ok: true},
		{header: "bytes=-600", totalSize: 500, lo: 0, hi: 499, ok: true},
		{header: "bytes=0-0", totalSize: 500, lo: 0, hi: 0, ok: true},
		{header: "not-a-range", totalSize: 500, lo: 0, hi: 0, ok: false},
		{header: "bytes=500-600", totalSize: 500, lo: 0, hi: 0, ok: false},
	}

	for _, tc := range tcs {
		t.Logf("test case: %s (totalSize=%d)", tc.header, tc.totalSize)
		lo, hi, ok := parseRangeRequestHeader(tc.header, tc.totalSize)
		assert.Equal(t, tc.ok, ok)
		assert.Equal(t, tc.lo, lo)
		assert.Equal(t, tc.hi, hi)
	}
}
