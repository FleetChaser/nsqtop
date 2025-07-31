package main

import "fmt"

func testSparkline() {
	history := []int{0, 10, 20, 30, 40, 50, 40, 30, 20, 10, 0}
	sparkline := generateSparkline(history)
	fmt.Printf("Sparkline: [%s]\n", sparkline)
	fmt.Printf("Length: %d runes\n", len([]rune(sparkline)))
	fmt.Printf("Characters: ")
	for _, r := range sparkline {
		fmt.Printf("%c ", r)
	}
	fmt.Println()
}
