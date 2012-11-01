package rediscluster

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
    "hash/crc32"
)

var (
    CLUSTER_NONINITIALIZED = iota
    CLUSTER_READY
    CLUSTER_DAMAGED
    CLUSTER_DOWN
)

type RedisCluster struct {
    Shards []*RedisShard
    NumShards int
    Status int
    initialized bool
}

func (rc *RedisCluster) AddShard(shard RedisShard) bool {
    if rc.initialized {
        return false
    }
    rc.Shards = append(rc.Shards, shard)
    rc.NumShards += 1
    rc.Status = rc.GetStatus()
    return true
}

func (rc *RedisCluster) GetStatus() int {
    if ! rc.initialized {
        return CLUSTER_NONINITIALIZED
    }
    allUp, allDown, someDown = true, true, false
    for shard := range rc.Shards {
        if shard.Status != REDIS_CONNECTED {
            shard.Conenct()
        }
        allUp &= (shard.Status == REDIS_CONNECTED)
        someDown |= (shard.Status != REDIS_CONNECTED)
        allDown &= (shard.Status != REDIS_CONNECTED)
    }
    if allUp && ! someDown {
        return CLUSTER_READY
    } else if allUp && someDown {
        return CLUSTER_DAMAGED
    } else if allDown {
        return CLUSTER_DOWN
    }
}

func (rc *RedisCluster) Partition(key string) (*redisShard, int) {
    idx := crc32.ChecksumIEEE([]byte(key)) % NumShards
    return rc.Shards[idx], idx
}

