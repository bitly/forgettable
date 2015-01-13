package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

type Value struct {
	Count int     `json:"count"`
	P     float64 `json:"p"`
}

type ValueMap map[string]*Value

func (vm ValueMap) MarshalJSON() ([]byte, error) {
	result := make([]map[string]interface{}, 0, len(vm))
	for bin, k := range vm {
		r := make(map[string]interface{})
		r["bin"] = bin
		r["count"] = k.Count
		r["p"] = k.P
		result = append(result, r)
	}
	return json.Marshal(result)
}

type Distribution struct {
	Name       string `json:"distribution"`
	Z          int    `json:"Z"`
	T          int
	Data       ValueMap `json:"data"`
	Rate       float64  `json:"rate"`
	Prune      bool     `json:"prune"`
	LastSyncT  int      `json:"last_sync_time"`
	numEntries int

	isFull     bool
	hasDecayed bool
}

func (d *Distribution) GetNMostProbable(N int) error {
	data, err := GetNMostProbable(d.Name, N)
	if err != nil || len(data) != 4 {
		return fmt.Errorf("Could not fetch data for %s: %s", d.Name, err)
	}

	d.numEntries, _ = redis.Int(data[1], nil)
	d.Z, _ = redis.Int(data[2], nil)
	d.T, _ = redis.Int(data[3], nil)
	d.Data = make(map[string]*Value)
	d.LastSyncT = d.T

	d.addMultiBulkCounts(data[0])
	return nil
}

func (d *Distribution) GetField(fields ...string) error {
	data, err := GetField(d.Name, fields...)

	N := len(fields)
	if err != nil || len(data) != 3+N {
		return fmt.Errorf("Could not retrieve field")
	}

	d.numEntries, _ = redis.Int(data[N], nil)
	d.Z, _ = redis.Int(data[N+1], nil)
	d.T, _ = redis.Int(data[N+2], nil)
	d.LastSyncT = d.T

	d.Data = make(map[string]*Value)
	var count int
	for i, field := range fields {
		count, _ = redis.Int(data[i], nil)
		d.Data[field] = &Value{Count: count}
	}
	d.calcProbabilities()

	return nil
}

func (d *Distribution) Fill() error {
	data, err := GetDistribution(d.Name)
	if err != nil {
		return fmt.Errorf("Could not fetch data for %s: %s", d.Name, err)
	}
	if data[0] == nil {
		return nil
	}

	T, err := redis.Int(data[0], nil)
	if err != nil {
		log.Printf("Could not read _T from distribution %s: %s", d.Name, err)
	}
	d.T = T
	d.LastSyncT = d.T

	// TODO: don't use the dist map to speed things up!
	d.Data = make(map[string]*Value)
	d.Rate = *defaultRate
	d.numEntries = len(data)

	d.addMultiBulkCounts(data[1])
	d.isFull = true

	d.Normalize()
	d.calcProbabilities()
	return nil
}

func (d *Distribution) addMultiBulkCounts(data interface{}) error {
	distData, _ := redis.MultiBulk(data, nil)
	for i := 0; i < len(distData); i += 2 {
		k, err := redis.String(distData[i], nil)
		if err != nil || k == "" {
			log.Printf("Could not read %s from distribution %s: %s", distData[i], d.Name, err)
		}
		v, err := redis.Int(distData[i+1], nil)
		if err != nil {
			log.Printf("Could not read %s from distribution %s: %s", distData[i+1], d.Name, err)
		}
		d.Data[k] = &Value{Count: v}
	}

	return nil
}

func (d *Distribution) Full() bool {
	return d.isFull
}

func (d *Distribution) HasDecayed() bool {
	return d.hasDecayed
}

func (d *Distribution) Normalize() {
	newZ := 0
	for _, v := range d.Data {
		newZ += v.Count
	}

	d.Z = newZ
	d.calcProbabilities()
}

func (d *Distribution) calcProbabilities() {
	fZ := float64(d.Z)
	for idx, _ := range d.Data {
		if fZ == 0 {
			d.Data[idx].P = 0
		} else {
			d.Data[idx].P = float64(d.Data[idx].Count) / fZ
		}
	}
}

func (d *Distribution) Decay() {
	if len(d.Data) == 0 {
		return
	}

	startingZ := d.Z
	now := time.Now()
	Z := 0
	sumDecay := 0
	for k, _ := range d.Data {
		l := DecayTime(d.Z, d.T, d.Rate, now)
		if l >= d.Data[k].Count {
			if d.Prune {
				l = d.Data[k].Count
			} else {
				l = d.Data[k].Count - 1
			}
		}
		sumDecay += l
		d.Data[k].Count -= l
		Z += d.Data[k].Count
	}
	if d.isFull {
		d.Z = Z
	} else {
		d.Z -= (sumDecay) / len(d.Data) * d.numEntries
		if d.Z < 0 {
			d.Z = 0
		}
	}

	if !d.hasDecayed && startingZ != d.Z {
		d.hasDecayed = true
	}

	d.T = int(now.Unix())

	d.calcProbabilities()
}
