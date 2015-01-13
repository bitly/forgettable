package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"math"
	"net/url"
	"strings"
	"time"
)

var (
	DistributionEmpty = fmt.Errorf("Distribution already empty, not updating")
)

type RedisServer struct {
	Host string
	Port string
	Db   string
	Pass string

	hostname string
	pool     *redis.Pool
}

func NewRedisServerFromRaw(rawString string) *RedisServer {
	parts := strings.Split(rawString, ":")
	if len(parts) != 3 {
		log.Fatal("redis-host must be in the form host:port:db")
	}
	rs := &RedisServer{
		Host:     parts[0],
		Port:     parts[1],
		Db:       parts[2],
		hostname: parts[0] + ":" + parts[1],
	}
	return rs
}

func NewRedisServerFromUri(uriString string) *RedisServer {
	url, err := url.Parse(uriString)
	if err != nil {
		log.Fatal("redis-uri must be in the form redis://[:password@]hostname:port[/db_number]")
	}

	// host and port (for nil-case port, set default: 6379)
	parts := strings.Split(url.Host, ":")
	host := parts[0]
	port := "6379" //default case
	if len(parts) > 1 {
		port = parts[1]
	}
	hostname := host + ":" + port

	// database number (for nil-case db, set default: 0)
	db := "0"
	if url.Path != "" {
		db = strings.Split(url.Path, "/")[1]
	}

	// check for password
	password := ""
	if url.User != nil {
		password, _ = url.User.Password()
	}

	rs := &RedisServer{
		Host:     host,
		Port:     port,
		hostname: hostname,
		Pass:     password,
		Db:       db,
	}
	return rs
}

func (rs *RedisServer) GetConnection() redis.Conn {
	return rs.pool.Get()
}

func (rs *RedisServer) Connect(maxIdle int) {
	// set up the connection pool
	rs.connectPool(maxIdle)

	// verify the connection pool is valid before allowing program to continue
	conn := rs.GetConnection()
	_, err := conn.Do("PING")
	if err != nil {
		log.Fatal("Could not connect to Redis!")
	}
	conn.Close()
	log.Println("Connected to redis")
}

func (rs *RedisServer) connectPool(maxIdle int) {
	rs.pool = &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", rs.hostname)
			if err != nil {
				return nil, err
			}
			if rs.Pass != "" {
				if _, err := c.Do("AUTH", rs.Pass); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", rs.Db); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func UpdateRedis(readChan chan *Distribution, id int) error {
	var redisConn redis.Conn
	var now int
	lastStatusTime := int(time.Now().Unix())
	updateCount := 0
	for dist := range readChan {
		// Only do a update if we have all the data necissary or we expect
		// there to be a decay event
		now = int(time.Now().Unix())
		if dist.Full() || float64(now-dist.LastSyncT)*dist.Rate > 0.75 {
			redisConn = redisServer.GetConnection()
			err := UpdateDistribution(redisConn, dist)
			if err != nil {
				log.Printf("[%d] Failed to update: %s: %v: %s", id, dist.Name, redisConn.Err(), err.Error())
			}
			updateCount += 1
			if now-lastStatusTime > *UpdateOutputTime {
				rate := float64(updateCount) / float64(now-lastStatusTime)
				log.Printf("[%d] Performing redis updates at %e updates/second", id, rate)
				lastStatusTime = now
				updateCount = 0
			}
			redisConn.Close()
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

func GetField(distribution string, fields ...string) ([]interface{}, error) {
	rdb := redisServer.GetConnection()
	defer rdb.Close()

	rdb.Send("MULTI")
	for _, field := range fields {
		rdb.Send("ZSCORE", distribution, field)
	}
	rdb.Send("ZCARD", distribution)
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_Z"))
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	return data, err
}

func GetNMostProbable(distribution string, N int) ([]interface{}, error) {
	rdb := redisServer.GetConnection()
	defer rdb.Close()

	rdb.Send("MULTI")
	rdb.Send("ZREVRANGEBYSCORE", distribution, "+INF", "-INF", "WITHSCORES", "LIMIT", 0, N)
	rdb.Send("ZCARD", distribution)
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_Z"))
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	return data, err
}

func IncrField(distribution string, fields []string, N int) error {
	rdb := redisServer.GetConnection()
	defer rdb.Close()

	rdb.Send("MULTI")
	for _, field := range fields {
		rdb.Send("ZINCRBY", distribution, N, field)
	}
	rdb.Send("INCRBY", fmt.Sprintf("%s.%s", distribution, "_Z"), N*len(fields))
	rdb.Send("SETNX", fmt.Sprintf("%s.%s", distribution, "_T"), int(time.Now().Unix()))
	_, err := rdb.Do("EXEC")
	return err
}

func GetDistribution(distribution string) ([]interface{}, error) {
	rdb := redisServer.GetConnection()
	defer rdb.Close()

	rdb.Send("MULTI")
	rdb.Send("GET", fmt.Sprintf("%s.%s", distribution, "_T"))
	rdb.Send("ZRANGE", distribution, 0, -1, "WITHSCORES")
	data, err := redis.MultiBulk(rdb.Do("EXEC"))
	return data, err
}

func DBSize() (int, error) {
	rdb := redisServer.GetConnection()
	defer rdb.Close()

	data, err := redis.Int(rdb.Do("DBSIZE"))
	return data, err
}
