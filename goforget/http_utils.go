package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type HttpResponseJson struct {
	StatusCode int         `json:"status_code"`
	StatusTxt  string      `json:"status_txt"`
	Data       interface{} `json:"data"`
}

var (
	ERROR_RESPONSE = `{"status_code": 500,"data": null,"status_txt": "COULD_NOT_FORMAT_RESULT"}`
)

func HttpError(w http.ResponseWriter, statusCode int, statusTxt string) bool {
	w.WriteHeader(statusCode)

	response := HttpResponseJson{StatusCode: statusCode, StatusTxt: statusTxt}
	j, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintf(w, ERROR_RESPONSE)
		log.Printf("Could not format response: %s", err)
		return false
	}
	fmt.Fprintf(w, "%s", j)
	return true
}

func HttpResponse(w http.ResponseWriter, statusCode int, data interface{}) bool {
	w.WriteHeader(statusCode)

	response := HttpResponseJson{StatusCode: statusCode, Data: data}
	j, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintf(w, ERROR_RESPONSE)
		log.Printf("Could not format response: %s", err)
		return false
	}
	fmt.Fprintf(w, "%s", j)
	return true
}
