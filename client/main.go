package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	baseURL               = "http://localhost:9443/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MX0/fhir"
	patientEverythingPath = "patient/$everything"
)

var outputPrefix = flag.String("output_prefix", "", "prefix prepended to the default file name.")

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
			time.Sleep(5*time.Second)
		default:
			return []string{}, fmt.Errorf("got status %v, want 200", resp.Status)
		}
	}
}

func download(url, filename string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("got status \"%v\", want 200", resp.Status)
	}
	if app := resp.Header.Get("Content-Type"); app != "application/fhir+ndjson" {
		return fmt.Errorf("expect content type application/fhir+ndjson, got %v", app)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, body, 0660)
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
	if *outputPrefix == "" {
		log.Fatalf("empty output prefix")
	}
	url := fmt.Sprintf("%v/%v", baseURL, patientEverythingPath)
	cl, err := reqBulkData(url)
	if err != nil {
		log.Fatalf("failed to request bulk data from %v: %v", url, err)
	}
	fmt.Printf("content location: %v\n", cl)
	links, err := getBulkDataLinks(cl)
	if err != nil {
		log.Fatalf("failed to get bulk data links from %v: %v", cl, err)
	}
	for _, link := range links {
		name := *outputPrefix + extractFilename(link)
		fmt.Printf("Downloading %v to %v...", link, name)
		if err := download(link, name); err != nil {
			fmt.Printf("\n")
			log.Fatalf("failed to download %v to %v: %v", link, name, err)
		}
		fmt.Printf(" Done\n")
	}
}
