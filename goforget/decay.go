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
	e := math.Exp(-1.0 * lambda)
	if e < 1e-8 {
		return math.MaxInt32
	}

	r := rand.Float64()
	k := int(0)
	p := e
	for p < r {
		k += 1
		e *= lambda / float64(k)
		p += e
	}
	return k
}

func Decay(count, Z, t int, rate float64) int {
	if count < 1 {
		return 0.0
	}

	now := int(time.Now().Unix())
	dt := (now - t)

	lambda := rate * float64(dt)
	k := Poisson(lambda)

	return k
}
