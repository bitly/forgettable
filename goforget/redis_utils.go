package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

var rdb redis.Conn
var rLock sync.RWMutex

func UpdateRedis(readChan chan *Distribution) {
	var err error
	for dist := range readChan {
		log.Printf("Updating distribution: %s", dist.Name)
		if dist.Data == nil {
			dist.Fill()
			if err != nil {
				log.Printf("Could not update %s: %s", dist.Name, err)
				continue
			}
			dist.Decay()
		}

		if dist.Z == 0 {
			continue
		}

		maxCount := 0
		rLock.Lock()
		rdb.Send("MULTI")
		for k, v := range dist.Data {
			if v.Count == 0 {
				rdb.Send("HDEL", dist.Name, k)
			} else {
				rdb.Send("HSET", dist.Name, k, v.Count)
				if v.Count > maxCount {
					maxCount = v.Count
				}
			}
		}

		rdb.Send("HSET", dist.Name, "_Z", dist.Z)
		rdb.Send("HSET", dist.Name, "_T", dist.T)

		eta := math.Sqrt(float64(maxCount) / dist.Rate)
		expTime := ((*expirSigma) + eta) * eta

		rdb.Send("EXPIRE", dist.Name, expTime)

		_, err := rdb.Do("EXEC")
		rLock.Unlock()
		if err != nil {
			log.Printf("Could not update %s: %s", dist.Name, err)
		}
	}
}

func GetField(distribution, field string) ([]interface{}, error) {
	rLock.RLock()
	data, err := redis.MultiBulk(rdb.Do("HMGET", distribution, field, "_Z", "_T"))
	rLock.RUnlock()
	return data, err
}

func IncrField(distribution, field string, N int) error {
	rLock.Lock()
	rdb.Send("MULTI")
	rdb.Send("HINCRBY", distribution, field, N)
	rdb.Send("HINCRBY", distribution, "_Z", N)
	rdb.Send("HSETNX", distribution, "_T", int(time.Now().Unix()))
	_, err := rdb.Do("EXEC")
	rLock.Unlock()
	return err
}

func GetDistribution(distribution string) ([]interface{}, error) {
	rLock.RLock()
	data, err := redis.MultiBulk(rdb.Do("HGETALL", distribution))
	rLock.RUnlock()
	return data, err
}

func ConnectRedis(host string) error {
	parts := strings.Split(host, ":")

	if len(parts) != 3 {
		log.Panicf("redis-host must be in the form host:port:db")
	}

	var err error
	rdb, err = redis.Dial("tcp", fmt.Sprintf("%s:%s", parts[0], parts[1]))
	if err == nil {
		ok, err := rdb.Do("SELECT", parts[2])
		if ok != "OK" || err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}
