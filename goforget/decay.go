package main

import (
	"log"
	"math"
	"math/rand"
	"time"
)

func Poisson(lambda float64) int {
	if lambda == 0.0 {
		return 0
	}
	e := math.Exp(-1.0 * lambda)
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

func Decay(Z, t int, rate float64) int {
	return DecayTime(Z, t, rate, time.Now())
}

func DecayTime(Z, t int, rate float64, now time.Time) int {
	dt := int(now.Unix()) - t

	lambda := rate * float64(dt)
	k := Poisson(lambda)

	if k == -1 {
		log.Printf("Poisson simulation did not converge with rate = %f => lambda = %f", rate, lambda)
		return 0
	}

	return k
}
