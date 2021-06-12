package bttest

import (
	"math/rand"
	"testing"
	"time"
)

func TestMergeRanges(t *testing.T) {
	// disjoint, start overlap, end overlap, equal, fully contained
	type rangeString struct {
		start string
		end   string
	}

	tcs := []struct {
		desc string
		a, b rangeString
		want *rangeString
	}{
		{"disjoint",
			rangeString{"a", "b"}, rangeString{"c", "d"}, nil},

		{"disjoint infinite",
			rangeString{"", "b"}, rangeString{"c", ""}, nil},

		{"same start",
			rangeString{"a", "b"}, rangeString{"a", "d"}, &rangeString{"a", "d"}},

		{"same start infinite",
			rangeString{"", "b"}, rangeString{"", "d"}, &rangeString{"", "d"}},

		{"same end",
			rangeString{"a", "d"}, rangeString{"c", "d"}, &rangeString{"a", "d"}},

		{"same end infinite",
			rangeString{"a", ""}, rangeString{"c", ""}, &rangeString{"a", ""}},

		{"eq",
			rangeString{"a", "d"}, rangeString{"a", "d"}, &rangeString{"a", "d"}},

		{"eq start infinite",
			rangeString{"", "d"}, rangeString{"", "d"}, &rangeString{"", "d"}},

		{"eq end infinite",
			rangeString{"a", ""}, rangeString{"a", ""}, &rangeString{"a", ""}},

		{"eq both infinite",
			rangeString{"", ""}, rangeString{"", ""}, &rangeString{"", ""}},

		{"a contains b",
			rangeString{"a", "d"}, rangeString{"b", "c"}, &rangeString{"a", "d"}},

		{"a contains b start infinite",
			rangeString{"", "d"}, rangeString{"b", "c"}, &rangeString{"", "d"}},

		{"a contains b end infinite",
			rangeString{"a", ""}, rangeString{"b", "c"}, &rangeString{"a", ""}},

		{"a contains b both infinite",
			rangeString{"", ""}, rangeString{"b", "c"}, &rangeString{"", ""}},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			result := mergeSimpleRanges([]simpleRange{{keyType(tc.a.start), keyType(tc.a.end)}, {keyType(tc.b.start), keyType(tc.b.end)}})
			if tc.want == nil {
				if len(result) != 2 {
					t.Errorf("expected to not merge, was %d %+v", len(result), result)
				}
			} else {
				if len(result) != 1 {
					t.Errorf("expected merge, was %d %+v", len(result), result)
				} else {
					got := result[0]
					if tc.want.start != string(got.start) {
						t.Errorf("start want=%q, got=%q", tc.want.start, string(got.start))
					}
					if tc.want.end != string(got.end) {
						t.Errorf("end want=%q, got=%q", tc.want.end, string(got.end))
					}
				}

			}
		})
	}
}

func TestMergeRangesMultiple(t *testing.T) {
	got := mergeSimpleRanges(nil)
	if len(got) != 0 {
		t.Errorf("want=0, got=%d", len(got))
	}

	in := []simpleRange{
		{keyType(""), keyType("a")},
		{keyType("a"), keyType("b")}, // merges
		{keyType("c"), keyType("e")},
		{keyType("d"), keyType("e")}, // merges
		{keyType("f"), keyType("i")},
		{keyType("g"), keyType("h")}, // merges
		{keyType("j"), keyType("k")},
		{keyType("k"), keyType("")}, // merges
	}

	want := []simpleRange{
		{keyType(""), keyType("b")},
		{keyType("c"), keyType("e")},
		{keyType("f"), keyType("i")},
		{keyType("j"), keyType("")},
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd.Shuffle(len(in), func(i, j int) {
		in[i], in[j] = in[j], in[i]
	})
	got = mergeSimpleRanges(in)
	if len(got) != len(want) {
		t.Fatalf("want=%d, got=%d", len(got), len(want))
	}

	for i := range want {
		want := want[i]
		got := got[i]
		if string(want.start) != string(got.start) {
			t.Errorf("start want=%q, got=%q", string(want.start), string(got.start))
		}
		if string(want.end) != string(got.end) {
			t.Errorf("end want=%q, got=%q", string(want.end), string(got.end))
		}
	}
}
