package main

import (
	"testing"
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
