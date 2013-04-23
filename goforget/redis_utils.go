package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"math"
	"strings"
	"time"
)

var (
	DistributionEmpty = fmt.Errorf("Distribution already empty, not updating")
)

type RedisServer struct {
	Raw  string
	Host string
	Port string
	Db   string

	hostname string
}

func NewRedisServer(rawString string) *RedisServer {
	parts := strings.Split(rawString, ":")
	if len(parts) != 3 {
		log.Panicf("redis-host must be in the form host:port:db")
	}
	return &RedisServer{
		Raw:      rawString,
		Host:     parts[0],
		Port:     parts[1],
		Db:       parts[2],
		hostname: parts[0] + ":" + parts[1],
	}
}

func (rs *RedisServer) Connect() (redis.Conn, error) {
	rdb, err := redis.Dial("tcp", rs.hostname)
	if err == nil {
		ok, err := rdb.Do("SELECT", rs.Db)
		if ok != "OK" || err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	return rdb, nil
}

var redisServer *RedisServer

func UpdateRedis(readChan chan *Distribution, id int) error {
	lredis, err := redisServer.Connect()
	if err != nil {
		log.Printf("Could not connect to redis host: %s: %s", redisServer.Raw, err)
		return err
	}

	for dist := range readChan {
		log.Printf("[%d] Updating distribution: %s", id, dist.Name)
		err := UpdateDistribution(lredis, dist)
		if err != nil {
			log.Printf("[%d] Failed to update: %s: %s", id, dist.Name, err.Error())
		}
	}
	return nil
}

func UpdateDistribution(rconn redis.Conn, dist *Distribution) error {
	ZName := fmt.Sprintf("%s.%s", dist.Name, "_Z")
	TName := fmt.Sprintf("%s.%s", dist.Name, "_T")

	rconn.Send("WATCH", ZName)
	defer rconn.Send("UNWATCH")

	if dist.Full() == false {
		err := dist.Fill()
		if err != nil {
			return fmt.Errorf("Could not fill: %s", err)
		}
		dist.Decay()
		dist.Normalize()
	}

	maxCount := 0
	rconn.Send("MULTI")
	if dist.HasDecayed() == true {
		if dist.Z == 0 {
			rconn.Send("DISCARD")
			return DistributionEmpty
		}

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
	} else {
		for _, v := range dist.Data {
			if v.Count != 0 && v.Count > maxCount {
				maxCount = v.Count
			}
		}
	}

	eta := math.Sqrt(float64(maxCount) / dist.Rate)
	expTime := int(((*expirSigma) + eta) * eta)

	rconn.Send("EXPIRE", dist.Name, expTime)
	rconn.Send("EXPIRE", ZName, expTime)
	rconn.Send("EXPIRE", TName, expTime)

	_, err := rconn.Do("EXEC")
	if err != nil {
		return fmt.Errorf("Could not update %s: %s", dist.Name, err)
	}
	return nil
}

func GetField(distribution, field string) ([]interface{}, error) {
	rdb, err := redisServer.Connect()
	if err != nil {
		return nil, err
	}

	rdb.Send("MULTI")
	rdb.Send("ZSCORE", distribution, field)
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_Z"))
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	return data, err
}

func GetNMostProbable(distribution string, N int) ([]interface{}, error) {
	rdb, err := redisServer.Connect()
	if err != nil {
		return nil, err
	}

	rdb.Send("MULTI")
	rdb.Send("ZREVRANGEBYSCORE", distribution, "+INF", "-INF", "WITHSCORES", "LIMIT", 0, N)
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_Z"))
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	return data, err
}

func IncrField(distribution string, fields []string, N int) error {
	rdb, err := redisServer.Connect()
	if err != nil {
		return err
	}

	rdb.Send("MULTI")
	for _, field := range fields {
		rdb.Send("ZINCRBY", distribution, N, field)
	}
	rdb.Send("INCRBY", fmt.Sprintf("%s.%s", distribution, "_Z"), N*len(fields))
	rdb.Send("SETNX", fmt.Sprintf("%s.%s", distribution, "_T"), int(time.Now().Unix()))
	_, err = rdb.Do("EXEC")
	return err
}

func GetDistribution(distribution string) ([]interface{}, error) {
	rdb, err := redisServer.Connect()
	if err != nil {
		return nil, err
	}

	rdb.Send("MULTI")
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	rdb.Send("ZRANGE", distribution, 0, -1, "WITHSCORES")
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	return data, err
}
