package main

import (
    "fmt"
    "log"
    "strings"
    "time"
	"github.com/garyburd/redigo/redis"
)


type Value struct {
    Count int `json:"count"`
    P float64   `json:"p"`
}

type Distribution struct {
	Name string `json:"distribution"`
	Z    int    `json:"Z"`
	T    int
	Data map[string]*Value `json:"data"`
	Rate float64        `json:"rate"`
}

func (d *Distribution) Fill() error {
    data, err := GetDistribution(d.Name)

	if err != nil {
		return fmt.Errorf("Could not fetch data for %s: %s", d.Name, err)
	}

	// TODO: don't use the dist map to speed things up!
	d.Data = make(map[string]*Value)
	d.Z = 0
	for i := 0; i < len(data); i += 2 {
		k, err := redis.String(data[i], nil)
		if err != nil || k == "" {
			log.Printf("Could not read %s from distribution %s: %s", data[i], d.Name, err)
		}
		if k == "_R" {
			var rate float64
			n, err := fmt.Fscan(strings.NewReader(data[i+1].(string)), &rate)
			if n == 1 && err == nil {
				d.Rate = rate
			}
		} else {
			v, err := redis.Int(data[i+1], nil)
			if err != nil {
				log.Printf("Could not read %s from distribution %s: %s", data[i+1], d.Name, err)
			}
			if k == "_Z" {
				continue
			} else if k == "_T" {
				d.T = v
			} else {
                d.Data[k] = &Value{Count:v}
				d.Z += v
			}
		}
	}

    fZ := float64(d.Z)
    for idx, _ := range d.Data {
        d.Data[idx].P = float64(d.Data[idx].Count) / fZ
    }

	return nil
}

func (d *Distribution) Decay() {
	newZ := 0
	for k, v := range d.Data {
		d.Data[k].Count, d.Z = Decay(v.Count, d.Z, d.T, d.Rate)
		newZ += d.Data[k].Count
	}

    fNewZ := float64(newZ)
    for idx, _ := range d.Data {
        d.Data[idx].P = float64(d.Data[idx].Count) / fNewZ
    }

    d.Z = newZ
	d.T = int(time.Now().Unix())
}

