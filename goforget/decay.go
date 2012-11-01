package main

import (
	"math"
	"math/rand"
	"time"
)

func factorial(n int) int {
	f := int(1)
	for ; n > 1; n -= 1 {
		f *= n
	}
	return f
}

func Poisson(lambda float64) int {
	rand.Seed(time.Now().UnixNano())
	r := rand.Float64()
	k := int(0)
	e := math.Exp(-1.0 * lambda)
	for p := 0.0; p < r; k += 1 {
		p += e * math.Pow(lambda, float64(k)) / float64(factorial(k))
	}
	return k
}

func Decay(count, Z, t int, rate float64) (int, int) {
	if count <= 1 {
		return count, Z
	}

	now := int(time.Now().Unix())
	dt := (now - t)

	lambda := rate * float64(dt)
	k := Poisson(lambda)

	if k > count {
		k = count
	}

	count, Z = count-k, Z-k
	return count, Z
}
