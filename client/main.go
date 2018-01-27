package main

import (
	"cloud.google.com/go/bigquery"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	bqProjectID  = flag.String("bq_project_id", "", "BigQuery project ID")
	url          = flag.String("url", "", "url to fetching the bulk data from")
	outputPrefix = flag.String("output_prefix", "", "prefix prepended to the default file name.")
)

func reqBulkData(url string) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Accept", "application/fhir+ndjson")
	req.Header.Add("Prefer", "respond-async")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		return "", fmt.Errorf("got status \"%v\", want 202", resp.Status)
	}
	return resp.Header.Get("Content-Location"), nil
}

func getBulkDataLinks(url string) ([]string, error) {
	// TODO: add timeout.
	for {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			ret := []string{}
			links := strings.Split(resp.Header.Get("Link"), ",")
			for _, link := range links {
				ret = append(ret, strings.Trim(link, "<>"))
			}
			return ret, nil
		case 202:
			time.Sleep(5 * time.Second)
		default:
			return []string{}, fmt.Errorf("got status %v, want 200", resp.Status)
		}
	}
}

func fetchBody(url) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return []byte{}, fmt.Errorf("got status \"%v\", want 200", resp.Status)
	}
	if app := resp.Header.Get("Content-Type"); app != "application/fhir+ndjson" {
		return []byte{}, fmt.Errorf("expect content type application/fhir+ndjson, got %v", app)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return body, nil
}

func extractFilename(path string) string {
	lastIndex := strings.LastIndex(path, "/")
	if lastIndex < 0 || lastIndex >= len(path)-1 {
		return path
	}
	return path[lastIndex+1:]
}

func main() {
	flag.Parse()
	cl, err := reqBulkData(*url)
	if err != nil {
		log.Fatalf("failed to request bulk data from %v: %v", *url, err)
	}
	fmt.Printf("content location: %v\n", cl)
	links, err := getBulkDataLinks(cl)
	if err != nil {
		log.Fatalf("failed to get bulk data links from %v: %v", cl, err)
	}
	if *bqProjectID != nil {
		ctx := context.Background()
		_, err := bigquery.NewClient(ctx, *bqProjectID)
		if err != nil {
			log.Fatalf("Failed to create BigQuery client: %v", err)
		}
	}
	for _, link := range links {
		fmt.Printf("Fetching %v...", link)
		body, err := fetchBody(link)
		if err != nil {
			log.Fatalf(" FAILED\n")
		}
		name := extractFilename(link)
		if *outputPrefix != "" {
			fmt.Printf(" Writing to %v...", name)
			ioutil.WriteFile(*outputPrefix+name, body, 0660)
			if err := download(link, name); err != nil {
				fmt.Printf(" FAILED\n")
				log.Fatalf("failed to download %v to %v: %v", link, name, err)
			}
		}
		fmt.Printf(" Done\n")
	}
}
