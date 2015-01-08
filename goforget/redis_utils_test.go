package main

import (
	"reflect"
	"testing"
)

var testUriCases = []struct {
	uriString   string
	expected    *RedisServer
	description string
}{
	{
		uriString: "redis://localhost:6379",
		expected: &RedisServer{
			Host:     "localhost",
			Port:     "6379",
			hostname: "localhost:6379",
			Db:       "0",
			Pass:     "",
		},
		description: "typical case",
	},
	{
		uriString: "redis://redisproviders.com:12345/4",
		expected: &RedisServer{
			Host:     "redisproviders.com",
			Port:     "12345",
			hostname: "redisproviders.com:12345",
			Db:       "4",
			Pass:     "",
		},
		description: "host, port, db",
	},
	{
		uriString: "redis://10.0.0.1",
		expected: &RedisServer{
			Host:     "10.0.0.1",
			Port:     "6379",
			hostname: "10.0.0.1:6379",
			Db:       "0",
			Pass:     "",
		},
		description: "host-only ip (guess default port and db)",
	},
	{
		uriString: "redis://10.0.0.1",
		expected: &RedisServer{
			Host:     "10.0.0.1",
			Port:     "6379",
			hostname: "10.0.0.1:6379",
			Db:       "0",
			Pass:     "",
		},
		description: "with password (no username)",
	},
	{
		uriString: "redis://oakland:ratchets@redis.bitly.com:999/1",
		expected: &RedisServer{
			Host:     "redis.bitly.com",
			Port:     "999",
			hostname: "redis.bitly.com:999",
			Db:       "1",
			Pass:     "ratchets",
		},
		description: "everything URI (username should be ignored)",
	},
}

var testRawCases = []struct {
	rawString   string
	expected    *RedisServer
	description string
}{
	{
		rawString: "localhost:6379:1",
		expected: &RedisServer{
			Host:     "localhost",
			Port:     "6379",
			hostname: "localhost:6379",
			Db:       "1",
			Pass:     "",
		},
		description: "typical case (all fields)",
	},
}

func TestNewRedisServerFromUri(t *testing.T) {
	for _, tt := range testUriCases {
		expected := tt.expected
		actual := NewRedisServerFromUri(tt.uriString)
		if reflect.DeepEqual(expected, actual) {
			t.Logf("PASS: %s", tt.description)
		} else {
			t.Errorf("FAIL: %s, expected: %+v, actual: %+v", tt.description, expected, actual)
		}
	}
}

func TestNewRedisServerFromRaw(t *testing.T) {
	for _, tt := range testRawCases {
		expected := tt.expected
		actual := NewRedisServerFromRaw(tt.rawString)
		if reflect.DeepEqual(expected, actual) {
			t.Logf("PASS: %s", tt.description)
		} else {
			t.Errorf("FAIL: %s, expected: %+v, actual: %+v", tt.description, expected, actual)
		}
	}
}
