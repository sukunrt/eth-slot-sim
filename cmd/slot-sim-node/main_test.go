package main

import (
	"slices"
	"testing"
	"time"
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

func TestSettleWindow(t *testing.T) {
	// A 150s startup leaves 120s to mesh after the 30s worst-case join stagger.
	if got := settleWindow(150*time.Second, meshJoinStagger); got != 120*time.Second {
		t.Errorf("settleWindow(150s, 30s) = %v, want 120s", got)
	}
	// startup <= stagger leaves no settle time — main rejects this.
	if got := settleWindow(20*time.Second, meshJoinStagger); got > 0 {
		t.Errorf("settleWindow(20s, 30s) = %v, want <= 0", got)
	}
}
