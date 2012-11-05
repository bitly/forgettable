package main

import (
	"math"
	"math/rand"
	"time"
)

func Poisson(lambda float64) int {
	rand.Seed(time.Now().UnixNano())
	if lambda == 0.0 {
		return 0
	}
	r := rand.Float64()
	k := int(0)
	e := math.Exp(-1.0 * lambda)
	p := e
	for p < r {
		k += 1
		e *= lambda / float64(k)
		p += e
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
		k = count - 1
	}

	count, Z = count-k, Z-k
	return count, Z
}
