package main

import (
	"log"
	"math"
	"math/rand"
	"time"
)

var MAX_ITER = 1000

func Poisson(lambda float64, seed int64) int {
	if lambda == 0.0 {
		return 0
	}
	e := math.Exp(-1.0 * lambda)
	if e < 1e-8 {
		return math.MaxInt32
	}

	rand.Seed(seed)
	counter := MAX_ITER
	r := rand.Float64()
	k := int(0)
	p := e
	for p < r {
		k += 1
		e *= lambda / float64(k)
		p += e
		if counter == 0 {
			return -1
		}
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
	k := Poisson(lambda, time.Now().UnixNano())

	if k == -1 {
		log.Printf("Poisson simulation did not converge with rate = %f => lambda = %f", rate, lambda)
		return 0
	}

	return k
}
