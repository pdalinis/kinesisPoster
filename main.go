// TODO: Handle throughput exceeded error from kinesis
// TODO: Persist shard iterator
// TODO: support any number of shards
// TODO: Post async

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	stream      = flag.String("stream", "", "The name of the Kinesis stream to read from")
	shardID     = flag.String("shardID", "shardId-000000000000", "The ShardID to read from")
	maxRecords  = flag.Int64("maxRecords", 50, "The number of Kinesis records to retrieve in each iteration, max of 10,000")
	filtersFile = flag.String("filters", "filters.json", "The path and name of the filters json file")
	region      = flag.String("region", "us-west-2", "The aws region")

	signals  = make(chan os.Signal, 1)
	shutdown = make(chan bool, 1)
)

func main() {
	flag.Parse()

	filters, err := loadFilters(*filtersFile)
	if err != nil {
		panic(fmt.Errorf("Could not load filters json file %v", err))
	}

	client := kinesis.New(session.New(), &aws.Config{Region: aws.String(*region)})

	shardIterator, err := getIterator(client)
	if err != nil {
		panic(fmt.Errorf("Could not get iterator from Kinesis %v", err))
	}

	log.Printf("Starting with shard iterator %s\n", *shardIterator)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	input := &kinesis.GetRecordsInput{
		Limit: maxRecords,
	}

	go func() {
		for {
			input.ShardIterator = shardIterator
			resp, err := client.GetRecords(input)

			if err != nil {
				log.Printf("Error calling GetRecords: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			shardIterator = resp.NextShardIterator

			var msg map[string]interface{}

			for _, r := range resp.Records {
				if err := json.Unmarshal(r.Data, &msg); err != nil {
					log.Printf("Could not unmarshal kinesis data %v", err)
					continue
				}

				url := filters.findUrl(msg)
				if url != "" {
					post(url, *r.SequenceNumber, r.Data)
				}

				msg = nil
			}

			select {
			case <-signals:
				log.Print("Received sigterm signal, shutting down...")
				shutdown <- true
				return
			default:
				time.Sleep(500 * time.Millisecond)
			}

		}
	}()

	<-shutdown
}

func post(url, sequence string, data []byte) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))

	if err != nil {
		log.Printf("Error posting to %v for sequence number %v- %v", url, sequence, err)
		return
	}
	if resp.StatusCode > 200 || resp.StatusCode >= 300 {
		log.Printf("Received status code %d when posting to %v for sequence number %v- %v", resp.StatusCode, url, sequence, err)
	}
	defer resp.Body.Close()
}

func getIterator(client *kinesis.Kinesis) (*string, error) {
	sinput := &kinesis.GetShardIteratorInput{
		ShardId:           shardID,
		ShardIteratorType: aws.String("AT_TIMESTAMP"),
		Timestamp:         aws.Time(time.Now()),
		StreamName:        stream,
	}

	sout, err := client.GetShardIterator(sinput)
	if err != nil {
		return nil, err
	}

	return sout.ShardIterator, nil
}
