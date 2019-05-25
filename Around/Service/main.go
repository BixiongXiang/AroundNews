package main

import (
	"encoding/json" // for encoding and decoding json
	"fmt"
	"log"
	"net/http" // for http request
	"strconv"
)

const (
	//DISTANCE is users distance
	DISTANCE = "200km"
)

/*Location is the location of user*/
type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"long"`
}

//Post is the user who post
type Post struct {
	//`json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

func main() {
	fmt.Println("Starting-service")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)

	//start server
	log.Fatal(http.ListenAndServe(":8080", nil))

	fmt.Println("Started-service")
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Recevied a post request")

	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)
		return
	}
	// fmt.Fprintf(w, "Post received: %s \n", p.Message) // never reach
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Recevied a search request")
	// lat := r.URL.Query().Get("lat")
	// lon := r.URL.Query().Get("lon")

	//convert the parameter form the request url
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Println("Range is ", ran) // use  , for println

	//return a fake post
	p := &Post{ // this is a structure
		User:    "1111",
		Message: "一生必去的100个地方",
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	js, err := json.Marshal(p) // encoding p to a json byte code

	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)

	// fmt.Fprintf(w, "Search received: %s %s", lat, lon)
}
