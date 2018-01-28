package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	linksInBody  = flag.Bool("links_in_body", true, "whether the download")
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

func getLinksFromHeader(resp *http.Response) []string {
	ret := []string{}
	links := strings.Split(resp.Header.Get("Link"), ",")
	for _, link := range links {
		ret = append(ret, strings.Trim(link, "<>"))
	}
	return ret
}

func unmarshalForLinks(body json.RawMessage) ([]string, error) {
	var jb map[string]interface{}
	if err := json.Unmarshal(body, &jb); err != nil {
		return []string{}, fmt.Errorf("unmarshal body: %v", err)
	}
	output, ok := jb["output"]
	if !ok {
		return []string{}, fmt.Errorf("field \"output\" not found in response body")
	}
	array, ok := output.([]interface{})
	if !ok {
		return []string{}, fmt.Errorf("unmarshal output array")
	}
	ret := []string{}
	for _, element := range array {
		config := element.(map[string]interface{})
		ret = append(ret, config["url"].(string))
	}
	return ret, nil
}

func getLinksFromBody(resp *http.Response) ([]string, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{}, err
	}
	return unmarshalForLinks(body)
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
			if *linksInBody {
				links, err := getLinksFromBody(resp)
				if err != nil {
					return links, err
				} else {
					return links, nil
				}
			} else {
				return getLinksFromHeader(resp), nil
			} 
		case 202:
			fmt.Println("Not ready. Sleeping 5 seconds...")
			time.Sleep(5 * time.Second)
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
	if app := resp.Header.Get("Content-Type"); !strings.Contains(app, "application/fhir+ndjson") {
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
	cl, err := reqBulkData(*url)
	if err != nil {
		log.Fatalf("failed to request bulk data from %v: %v", *url, err)
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
