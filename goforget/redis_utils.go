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
				rdb.Send("ZREM", dist.Name, k)
			} else {
				rdb.Send("ZADD", dist.Name, v.Count, k)
				if v.Count > maxCount {
					maxCount = v.Count
				}
			}
		}

		ZName := fmt.Sprintf("%s.%s", dist.Name, "_Z")
		TName := fmt.Sprintf("%s.%s", dist.Name, "_T")

		rdb.Send("SET", ZName, dist.Z)
		rdb.Send("SET", TName, dist.T)

		eta := math.Sqrt(float64(maxCount) / dist.Rate)
		expTime := int(((*expirSigma) + eta) * eta)

		rdb.Send("EXPIRE", dist.Name, expTime)
		rdb.Send("EXPIRE", ZName, expTime)
		rdb.Send("EXPIRE", TName, expTime)

		_, err := rdb.Do("EXEC")
		rLock.Unlock()
		if err != nil {
			log.Printf("Could not update %s: %s", dist.Name, err)
		}
	}
}

func GetField(distribution, field string) ([]interface{}, error) {
	rLock.RLock()
	rdb.Send("MULTI")
	rdb.Send("ZSCORE", distribution, field)
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_Z"))
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	rLock.RUnlock()
	return data, err
}

func IncrField(distribution, field string, N int) error {
	rLock.Lock()
	rdb.Send("MULTI")
	rdb.Send("ZINCRBY", distribution, N, field)
	rdb.Send("INCRBY", fmt.Sprintf("%s.%s", distribution, "_Z"), N)
	rdb.Send("SETNX", fmt.Sprintf("%s.%s", distribution, "_T"), int(time.Now().Unix()))
	_, err := rdb.Do("EXEC")
	rLock.Unlock()
	return err
}

func GetDistribution(distribution string) ([]interface{}, error) {
	rLock.RLock()
	rdb.Send("MULTI")
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	rdb.Send("ZRANGE", distribution, 0, -1, "WITHSCORES")
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
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
