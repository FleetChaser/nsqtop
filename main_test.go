package main

import (
	"testing"
	"time"
)

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{123, "123"},
		{1234, "1,234"},
		{1234567, "1,234,567"},
		{0, "0"},
		{1, "1"},
		{12, "12"},
	}

	for _, test := range tests {
		result := formatNumber(test.input)
		if result != test.expected {
			t.Errorf("formatNumber(%d) = %s; expected %s", test.input, result, test.expected)
		}
	}
}

func TestGenerateSparkline(t *testing.T) {
	tests := []struct {
		input    []int
		expected int // length of output in runes
	}{
		{[]int{}, 0},
		{[]int{1, 2, 3}, 3},
		{[]int{0, 0, 0}, 3},
		{[]int{10, 20, 30, 40, 50}, 5},
	}

	for _, test := range tests {
		result := generateSparkline(test.input)
		// Count runes, not bytes, since we're using Unicode characters
		runeCount := len([]rune(result))
		if runeCount != test.expected {
			t.Errorf("generateSparkline(%v) rune length = %d; expected %d", test.input, runeCount, test.expected)
		}

		// Additional check: ensure we're getting valid sparkline characters
		if len(test.input) > 0 {
			for _, r := range result {
				found := false
				for _, sparklineRune := range SparklineChars {
					if r == sparklineRune {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("generateSparkline(%v) contains invalid character: %c", test.input, r)
				}
			}
		}
	}
}

func TestSortChannels(t *testing.T) {
	mk := func(topic string, depth, inflight int, inPerSec float64, processed int64) *ChannelData {
		return &ChannelData{Topic: topic, Channel: "c", Depth: depth, InFlightCount: inflight, IncomingPerSecond: inPerSec, MessageCount: processed}
	}
	a := mk("a", 10, 5, 1.0, 100)
	b := mk("b", 30, 1, 3.0, 300)
	c := mk("c", 20, 9, 2.0, 200)

	tests := []struct {
		name   string
		column int
		desc   bool
		want   []string // topics in expected order
	}{
		{"depth desc", sortColumnDepth, true, []string{"b", "c", "a"}},
		{"depth asc", sortColumnDepth, false, []string{"a", "c", "b"}},
		{"name asc", 0, false, []string{"a", "b", "c"}},
		{"inflight desc", 2, true, []string{"c", "a", "b"}},
		{"in/sec desc", 4, true, []string{"b", "c", "a"}},
		{"processed asc", 6, false, []string{"a", "c", "b"}},
	}

	for _, test := range tests {
		n := &NSQTop{sortColumn: test.column, sortDesc: test.desc}
		channels := []*ChannelData{a, b, c}
		n.sortChannels(channels)
		for i, wantTopic := range test.want {
			if channels[i].Topic != wantTopic {
				t.Errorf("%s: position %d = %q; want %q", test.name, i, channels[i].Topic, wantTopic)
			}
		}
	}
}

func TestChangeSortColumnWraps(t *testing.T) {
	n := &NSQTop{sortColumn: 0, sortDesc: false}
	n.changeSortColumn(-1)
	if n.sortColumn != len(columnTitles)-1 {
		t.Errorf("wrapping left from 0 should land on last column, got %d", n.sortColumn)
	}
	n.changeSortColumn(1)
	if n.sortColumn != 0 {
		t.Errorf("wrapping right from last should return to 0, got %d", n.sortColumn)
	}
	// Moving to a numeric column defaults to descending; the name column to ascending.
	n.changeSortColumn(1) // -> column 1 (Depth, numeric)
	if !n.sortDesc {
		t.Errorf("numeric column should default to descending")
	}
}

func TestAdjustInterval(t *testing.T) {
	newTop := func(start time.Duration) *NSQTop {
		n := &NSQTop{intervalCh: make(chan time.Duration, 1)}
		n.intervalNanos.Store(int64(start))
		return n
	}

	// One step faster / slower.
	n := newTop(5 * time.Second)
	n.adjustInterval(-IntervalStep)
	if got := n.getInterval(); got != 5*time.Second-IntervalStep {
		t.Errorf("faster: got %v; want %v", got, 5*time.Second-IntervalStep)
	}

	// Clamps at the minimum no matter how many times we speed up.
	n = newTop(1 * time.Second)
	for i := 0; i < 50; i++ {
		n.adjustInterval(-IntervalStep)
	}
	if got := n.getInterval(); got != MinInterval {
		t.Errorf("clamp min: got %v; want %v", got, MinInterval)
	}

	// Clamps at the maximum no matter how many times we slow down.
	n = newTop(1 * time.Second)
	for i := 0; i < 100; i++ {
		n.adjustInterval(IntervalStep)
	}
	if got := n.getInterval(); got != MaxInterval {
		t.Errorf("clamp max: got %v; want %v", got, MaxInterval)
	}
}

func TestFilterChannels(t *testing.T) {
	channels := []*ChannelData{
		{Topic: "fc.dvr.Event", Channel: "fleet-worker"},
		{Topic: "fc.tracker.ProcessReading", Channel: "fleet-worker"},
		{Topic: "chat.BroadcastMessage", Channel: "chat.instance#ephemeral"},
	}

	if got := filterChannels(channels, ""); len(got) != 3 {
		t.Errorf("empty query should return all; got %d", len(got))
	}
	if got := filterChannels(channels, "dvr"); len(got) != 1 || got[0].Topic != "fc.dvr.Event" {
		t.Errorf("query %q did not match expected single channel: %+v", "dvr", got)
	}
	if got := filterChannels(channels, "FLEET"); len(got) != 2 { // case-insensitive, matches channel name
		t.Errorf("case-insensitive query should match 2; got %d", len(got))
	}
	if got := filterChannels(channels, "nope"); len(got) != 0 {
		t.Errorf("non-matching query should return none; got %d", len(got))
	}
}

func TestFormatGrowth(t *testing.T) {
	if got := formatGrowth(1234, 0); got != "1,234" {
		t.Errorf("idle counter should show plain count, got %q", got)
	}
	if got := formatGrowth(1234, 0.01); got != "1,234" {
		t.Errorf("sub-threshold rate should not show marker, got %q", got)
	}
	if got := formatGrowth(1234, 2.5); got != "1,234 ▲2.5" {
		t.Errorf("growing counter should show marker, got %q", got)
	}
}

func TestNormalizeAddresses(t *testing.T) {
	got := normalizeAddresses(" localhost:4161 , http://h2:4161/ ,, https://h3:4161")
	want := []string{"http://localhost:4161", "http://h2:4161", "https://h3:4161"}
	if len(got) != len(want) {
		t.Fatalf("got %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("address %d = %q; want %q", i, got[i], want[i])
		}
	}
}

func TestGetEnvWithFallback(t *testing.T) {
	// Test with non-existent env var
	result := getEnvWithFallback("NON_EXISTENT_VAR", "default")
	if result != "default" {
		t.Errorf("getEnvWithFallback should return fallback value, got %s", result)
	}
}

func TestGetEnvIntWithFallback(t *testing.T) {
	// Test with non-existent env var
	result := getEnvIntWithFallback("NON_EXISTENT_INT_VAR", 42)
	if result != 42 {
		t.Errorf("getEnvIntWithFallback should return fallback value, got %d", result)
	}
}
