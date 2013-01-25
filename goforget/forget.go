package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

var (
	VERSION     = "0.1"
	showVersion = flag.Bool("version", false, "print version string")
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
	fields, ok := reqParams["field"]
	if !ok || len(fields) == 0 {
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

	err = IncrField(distribution, fields, N)
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
		dist.Normalize()
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

	result := Distribution{
		Name:  distribution,
		Rate:  rate,
		Prune: *pruneDist,
	}
	err = result.GetField(field)
	if err != nil {
		HttpError(w, 500, "COULD_NOT_RETRIEVE_FIELD")
		return
	}

	result.Decay()

	HttpResponse(w, 200, result)
	updateChan <- &result
}

func NMostProbableHandler(w http.ResponseWriter, r *http.Request) {
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
	N_raw := reqParams.Get("N")
	var N int
	if N_raw == "" {
		N = 10
	} else {
		N, err = strconv.Atoi(N_raw)
		if err != nil {
			HttpError(w, 500, "INVALID_ARG_N")
			return
		}
	}

	result := Distribution{
		Name:  distribution,
		Rate:  rate,
		Prune: *pruneDist,
	}
	result.GetNMostProbable(N)
	result.Decay()

	HttpResponse(w, 200, result)
	updateChan <- &result
}

func ExitHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
	Exit()
}

func Exit() {
	close(updateChan)
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("goforget: v%s\n", VERSION)
		return
	}

	var err error
	rdb, err = ConnectRedis(*redisHost)
	if err != nil {
		log.Printf("Could not connect to redis host: %s: %s", *redisHost, err)
		return
	} else {
		log.Printf("Connected to %s", *redisHost)
	}

	log.Printf("Starting %d update worker(s)", *nWorkers)
	workerWaitGroup := sync.WaitGroup{}
	updateChan = make(chan *Distribution, 10) //25 * *nWorkers)
	for i := 0; i < *nWorkers; i++ {
		workerWaitGroup.Add(1)
		go func() {
			UpdateRedis(*redisHost, updateChan)
			workerWaitGroup.Done()
		}()
	}

	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/incr", IncrHandler)
	http.HandleFunc("/dist", DistHandler)
	http.HandleFunc("/nmostprobable", NMostProbableHandler)
	http.HandleFunc("/exit", ExitHandler)
	go func() {
		log.Fatal(http.ListenAndServe(*httpAddress, nil))
	}()

	workerWaitGroup.Wait()
}
