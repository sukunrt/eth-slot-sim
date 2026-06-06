package main

import (
	"slices"
	"testing"
)

func TestParseIntList(t *testing.T) {
	tests := []struct {
		in   string
		want []int
	}{
		{"", nil},
		{"5", []int{5}},
		{"1,3,7", []int{1, 3, 7}},
		{" 1 , 2 ,3 ", []int{1, 2, 3}}, // tolerates whitespace
		{"4,,6", []int{4, 6}},          // tolerates empty fields
	}
	for _, tc := range tests {
		got := parseIntList(tc.in)
		if !slices.Equal(got, tc.want) {
			t.Errorf("parseIntList(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
