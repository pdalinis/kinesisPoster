package main

import (
	"encoding/json"
	"io/ioutil"
)

type Filters []*Filter

// Filter is a mapping of a json key and value to the URL that will be posted to.
type Filter struct {
	URL   string `json:"url"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func loadFilters(file string) (filters Filters, err error) {
	var js []byte

	js, err = ioutil.ReadFile(file)
	if err != nil {
		return
	}

	err = json.Unmarshal(js, &filters)
	return
}

func (f *Filters) findUrl(data map[string]interface{}) string {
	for _, filter := range *f {
		kv, ok := data[filter.Key]
		if !ok {
			continue
		}

		value, ok := kv.(string)
		if !ok {
			// TODO: Return an error?
			return ""
		}

		if value == filter.Value {
			return filter.URL
		}
	}
	// TODO: Return an error?
	return ""
}
