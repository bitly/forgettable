package main

import (
	"testing"
)

func TestFactorial(t *testing.T) {
	if factorial(5) != 120 {
		t.Errorf("Factorial failed... 5! != 120")
	}
}
