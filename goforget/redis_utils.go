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

func UpdateRedis(redisHost string, readChan chan *Distribution) {
	lredis, err := ConnectRedis(redisHost)
	if err != nil {
		log.Printf("Could not connect to redis host: %s: %s", redisHost, err)
		return
	}

	for dist := range readChan {
		log.Printf("Updating distribution: %s", dist.Name)
		ok := UpdateDistribution(lredis, dist)
		if !ok {
			log.Println("Failed to update: %s", dist.Name)
		}
	}
}

func UpdateDistribution(rconn redis.Conn, dist *Distribution) bool {
	ZName := fmt.Sprintf("%s.%s", dist.Name, "_Z")
	TName := fmt.Sprintf("%s.%s", dist.Name, "_T")

	rconn.Send("WATCH", ZName)
	defer rconn.Send("UNWATCH")

	if dist.Full() == false {
		err := dist.Fill()
		if err != nil {
			log.Printf("Could not update %s: %s", dist.Name, err)
			return false
		}
		dist.Decay()
		dist.Normalize()
	}

	if dist.HasDecayed() == false {
		log.Printf("Skipping update... nothing has changed: %s", dist.Name)
		return true
	}

	rLock.Lock()
	rconn.Send("MULTI")

	if dist.Z == 0 {
		rconn.Send("DISCARD")
		rLock.Unlock()
		return false
	}

	maxCount := 0
	for k, v := range dist.Data {
		if v.Count == 0 {
			rconn.Send("ZREM", dist.Name, k)
		} else {
			rconn.Send("ZADD", dist.Name, v.Count, k)
			if v.Count > maxCount {
				maxCount = v.Count
			}
		}
	}

	rconn.Send("SET", ZName, dist.Z)
	rconn.Send("SET", TName, dist.T)

	eta := math.Sqrt(float64(maxCount) / dist.Rate)
	expTime := int(((*expirSigma) + eta) * eta)

	rconn.Send("EXPIRE", dist.Name, expTime)
	rconn.Send("EXPIRE", ZName, expTime)
	rconn.Send("EXPIRE", TName, expTime)

	_, err := rconn.Do("EXEC")
	rLock.Unlock()
	if err != nil {
		log.Printf("Could not update %s: %s", dist.Name, err)
		return false
	}
	return true
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

func GetNMostProbable(distribution string, N int) ([]interface{}, error) {
	rLock.RLock()
	rdb.Send("MULTI")
	rdb.Send("ZREVRANGEBYSCORE", distribution, "+INF", "-INF", "WITHSCORES", "LIMIT", 0, N)
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

func ConnectRedis(host string) (redis.Conn, error) {
	parts := strings.Split(host, ":")

	if len(parts) != 3 {
		log.Panicf("redis-host must be in the form host:port:db")
	}

	rdb, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", parts[0], parts[1]))
	if err == nil {
		ok, err := rdb.Do("SELECT", parts[2])
		if ok != "OK" || err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	return rdb, nil
}
