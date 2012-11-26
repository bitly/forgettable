package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var (
	httpAddress = flag.String("http", ":8080", "HTTP service address (e.g., ':8080')")
	redisHost   = flag.String("redis-host", "", "Redis host in the form host:port:db.")
	defaultRate = flag.Float64("default-rate", 0.5, "Default rate to decay distributions with")
	nWorkers    = flag.Int("nworkers", 1, "Number of update workers that update the redis DB")
	pruneDist   = flag.Bool("prune", true, "Whether or not to decay distributional fields out")
	expirSigma  = flag.Float64("expire-sigma", 2, "Confidence level that a distribution will be empty when set to expire")
)

var updateChan chan *Distribution

func IncrHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}
	distribution := reqParams.Get("distribution")
	if distribution == "" {
		HttpError(w, 500, "MISSING_ARG_DISTRIBUTION")
		return
	}
	field := reqParams.Get("field")
	if field == "" {
		HttpError(w, 500, "MISSING_ARG_FIELD")
		return
	}
	N_raw := reqParams.Get("N")
	var N int
	if N_raw == "" {
		N = 1
	} else {
		N, err = strconv.Atoi(N_raw)
		if err != nil {
			HttpError(w, 500, "COULDNT_PARSE_N")
			return
		}
	}

	err = IncrField(distribution, field, N)
	if err == nil {
		w.WriteHeader(200)
		fmt.Fprintf(w, "OK")
	} else {
		w.WriteHeader(500)
		fmt.Fprintf(w, "FAIL")
	}
	updateChan <- &Distribution{Name: distribution}
}

func DistHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}
	distribution := reqParams.Get("distribution")
	if distribution == "" {
		HttpError(w, 500, "MISSING_ARG_DISTRIBUTION")
		return
	}
	var rate float64
	rate_raw := reqParams.Get("rate")
	if rate_raw == "" {
		rate = *defaultRate
	} else {
		n, err := fmt.Fscan(strings.NewReader(rate_raw), &rate)
		if n == 0 || err != nil {
			HttpError(w, 500, "CANNOT_PARSE_RATE")
			return
		}
	}

	dist := Distribution{
		Name:  distribution,
		Prune: *pruneDist,
	}
	err = dist.Fill()
	if err != nil {
		HttpError(w, 500, "COULD_NOT_RETRIEVE_DISTRIBUTION")
		return
	}

	if len(dist.Data) != 0 {
		if dist.Rate == *defaultRate {
			dist.Rate = rate
		}

		dist.Decay()
		updateChan <- &dist
	}

	HttpResponse(w, 200, dist)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}
	distribution := reqParams.Get("distribution")
	if distribution == "" {
		HttpError(w, 500, "MISSING_ARG_DISTRIBUTION")
		return
	}
	field := reqParams.Get("field")
	if field == "" {
		HttpError(w, 500, "MISSING_ARG_FIELD")
		return
	}
	var rate float64
	rate_raw := reqParams.Get("rate")
	if rate_raw == "" {
		rate = *defaultRate
	} else {
		n, err := fmt.Fscan(strings.NewReader(rate_raw), &rate)
		if n == 0 || err != nil {
			HttpError(w, 500, "CANNOT_PARSE_RATE")
			return
		}
	}

	data, err := GetField(distribution, field)

	if err != nil || len(data) != 3 {
		HttpError(w, 500, "COULD_NOT_RETRIEVE_FIELD")
		return
	}

	count, _ := redis.Int(data[0], nil)
	Z, _ := redis.Int(data[1], nil)
	t, _ := redis.Int(data[2], nil)

	l := Decay(count, Z, t, rate)
	if l >= count {
		if *pruneDist {
			l = count
		} else {
			l = count - 1
		}
	}
	count, Z = count-l, Z-l

	var p float64
	if Z == 0 {
		p = 0.0
	} else {
		p = float64(count) / float64(Z)
	}

	result := Distribution{
		Name:  distribution,
		Z:     Z,
		T:     t,
		Data:  map[string]*Value{field: &Value{Count: count, P: p}},
		Rate:  rate,
		Prune: *pruneDist,
	}

	HttpResponse(w, 200, result)
	updateChan <- &result
}

func main() {
	flag.Parse()

	err := ConnectRedis(*redisHost)
	if err != nil {
		log.Printf("Could not connect to redis host: %s: %s", *redisHost, err)
		return
	} else {
		log.Printf("Connected to %s", *redisHost)
	}

	log.Printf("Starting %d update workers", *nWorkers)
	updateChan = make(chan *Distribution, 10) //25 * *nWorkers)
	for i := 0; i < *nWorkers; i++ {
		go UpdateRedis(updateChan)
	}

	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/incr", IncrHandler)
	http.HandleFunc("/dist", DistHandler)
	log.Fatal(http.ListenAndServe(*httpAddress, nil))
}
