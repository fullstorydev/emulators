package gcsemu

import (
	"strconv"
	"strings"
)

type byteRange struct {
	lo, hi, sz int64
}

// parseRangeRequestHeader parses an HTTP Range request header (e.g. "bytes=0-99")
// and clamps to the given total size. Returns the inclusive lo/hi byte offsets
// and true if a valid range was parsed.
func parseRangeRequestHeader(header string, totalSize int64) (lo, hi int64, ok bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		lo = totalSize - suffix
		if lo < 0 {
			lo = 0
		}
		return lo, totalSize - 1, true
	}

	lo, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || lo < 0 || lo >= totalSize {
		return 0, 0, false
	}

	if parts[1] == "" {
		return lo, totalSize - 1, true
	}

	hi, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || hi < lo {
		return 0, 0, false
	}
	if hi >= totalSize {
		hi = totalSize - 1
	}
	return lo, hi, true
}

func parseByteRange(in string) *byteRange {
	var err error
	if !strings.HasPrefix(in, "bytes ") {
		return nil
	}
	in = strings.TrimPrefix(in, "bytes ")
	parts := strings.Split(in, "/")
	if len(parts) != 2 {
		return nil
	}

	ret := byteRange{
		lo: -1,
		hi: -1,
		sz: -1,
	}

	if parts[0] != "*" {
		parts := strings.Split(parts[0], "-")
		if len(parts) != 2 {
			return nil
		}
		ret.lo, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil
		}
		ret.hi, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil
		}
	}

	if parts[1] != "*" {
		ret.sz, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil
		}
	}

	return &ret
}
