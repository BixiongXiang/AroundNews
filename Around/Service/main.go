package main

import (
	"cloud.google.com/go/bigtable"
	"context" // for elasticsearch
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"

	"strconv"

	"github.com/olivere/elastic" // elastic search
	"github.com/pborman/uuid"

	"cloud.google.com/go/storage"
)

const (
	POST_INDEX = "post"
	POST_TYPE  = "post"
	DISTANCE   = "200km"

	// this is the URL for accessing the ES on GCE
	ES_URL = "http://35.236.110.147:9200"

	// this is the bucket I created on GCS to store user uploaded images
	BUCKET_NAME = "post-images-aroundx"

	//this is the GCP project ID
	BIGTABLE_PROJECT_ID = "fourth-banner-241502"
	//this is the bigtable instance's ID
	BT_INSTANCE = "around-post"
)

// structs
type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"` // url from GCS stored media
}

//functions
func main() {
	fmt.Println("started-service")

	//elastic search
	createIndexIfNotExist()

	//register http request handler, similar with servlet
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

// inital ES tell it how to store
func createIndexIfNotExist() {
	//connect ES with url and set sniff do data replication as false(need in distributed sys)
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	exists, err := client.IndexExists(POST_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if !exists {
		//tell search to use geo info to search
		mapping := `{
	"mappings": {
		"post": {
		    "properties": {
			"location": {
			    "type": "geo_point"
			}
		    }
		}
	    }
	}`

		_, err = client.CreateIndex(POST_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}

}

// Save a post to ElasticSearch, use ES as a DB to store and search data
func saveToES(post *Post, id string) error {
	//connect es
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return err
	}

	_, err = client.Index().
		Index(POST_INDEX).
		Type(POST_TYPE).
		Id(id).
		BodyJson(post).
		Refresh("wait_for").
		Do(context.Background())
	if err != nil {
		return err
	}

	fmt.Printf("Post is saved to index: %s\n", post.Message)
	return nil
}

func saveToBigTable(p *Post, id string) error {
	ctx := context.Background()
	bt_client, err := bigtable.NewClient(ctx, BIGTABLE_PROJECT_ID, BT_INSTANCE)
	if err != nil {
		return err
	}

	tbl := bt_client.Open("post")

	// mut to open a new row to store data
	mut := bigtable.NewMutation()
	t := bigtable.Now() // get current time stamp

	// big table accepts []byte form
	//[family]-[field]-[timeStamp(save multi version)]-[data]
	mut.Set("post", "user", t, []byte(p.User))
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	//write to Bigtable
	err = tbl.Apply(ctx, id, mut)
	if err != nil {
		return err
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
	return nil

}

// get data from ES
func readFromES(lat, lon float64, ran string) ([]Post, error) {
	//created a new client to connect to ES, also get err if happend
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}

	// query with location
	query := elastic.NewGeoDistanceQuery("location")
	query = query.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
		Index(POST_INDEX).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ptyp Post
	var posts []Post
	for _, item := range searchResult.Each(reflect.TypeOf(ptyp)) {
		if p, ok := item.(Post); ok {
			posts = append(posts, p)
		}
	}

	return posts, nil
}

// save files to GCS to store those media files
func saveToGCS(r io.Reader, bucketName, objectName string) (*storage.ObjectAttrs, error) {

	// for go concurrency
	ctx := context.Background() // more on context: https://blog.golang.org/context

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	// find our GCS bucket
	bucket := client.Bucket(bucketName)
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, err
	}

	// make our file a n object
	object := bucket.Object(objectName)
	wc := object.NewWriter(ctx)
	if _, err = io.Copy(wc, r); err != nil {
		return nil, err
	}
	if err := wc.Close(); err != nil {
		return nil, err
	}

	//access control, here the demo set for everyone
	if err = object.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, err
	}

	//this is returned attribute from GCS, include the url
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Image is saved to GCS: %s\n", attrs.MediaLink)
	return attrs, nil
}

//handlers for the server
func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User:    r.FormValue("user"),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()
	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusBadRequest)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}

	//save medium to GCS
	attrs, err := saveToGCS(file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "Failed to save image to GCS", http.StatusInternalServerError)
		fmt.Printf("Failed to save image to GCS %v.\n", err)
		return
	}
	p.Url = attrs.MediaLink

	//save post info to Elastic Search
	err = saveToES(p, id)
	if err != nil {
		http.Error(w, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
		return
	}
	fmt.Printf("Saved one post to ElasticSearch: %s\n", p.Message)

	//save a dup to Big table
	// in order to save the cost of big table, I stoped saving to bigtable here
	/*
		err = saveToBigTable(p, id)
		if err != nil {
			http.Error(w, "Failed to save post to BigTable", http.StatusInternalServerError)
			fmt.Printf("Failed to save post to BigTable %v.\n", err)
			return
		}
	*/

	//try to add some feed back message in request
	post := struct {
		Status  string
		Message string
	}{"succeed", p.Message}
	js, err := json.Marshal(post)
	w.Write(js)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	if r.Method == "OPTIONS" {
		return
	}

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	// get data from ES within the circle
	posts, err := readFromES(lat, lon, ran)
	if err != nil {
		http.Error(w, "Failed to read post from ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to read post from ElasticSearch %v.\n", err)
		return
	}

	js, err := json.Marshal(posts)
	if err != nil {
		http.Error(w, "Failed to parse posts into JSON format", http.StatusInternalServerError)
		fmt.Printf("Failed to parse posts into JSON format %v.\n", err)
		return
	}

	w.Write(js)
}
