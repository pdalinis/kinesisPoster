// TODO: Persist shard iterator off filesystem
// TODO: support any number of shards
// TODO: Post async

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
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

	var shardIterator *string
	sequenceNumber := getSequenceNumberFromDisk()

	if sequenceNumber == "" {
		shardIterator, err = getIteratorFromNow(client)
		if err != nil {
			panic(fmt.Errorf("Could not get starting iterator from Kinesis %v", err))
		}
	} else {
		shardIterator, err = getIteratorFromSequenceNumber(sequenceNumber, client)
		if err != nil {
			panic(fmt.Errorf("Could not get iterator from Kinesis for sequence number %s - %v", sequenceNumber, err))
		}
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
				time.Sleep(1100 * time.Millisecond)
				continue
			}

			if resp.NextShardIterator == nil {
				log.Print("NextShardIterator is null, stream has been closed, shutting down")
				signals <- syscall.SIGTERM
			} else {
				shardIterator = resp.NextShardIterator
				if *resp.MillisBehindLatest > 5000 {
					log.Printf("More than 5 seconds behind")
				}
			}

			var msg map[string]interface{}

			for _, r := range resp.Records {
				msg = nil
				if err := json.Unmarshal(r.Data, &msg); err != nil {
					log.Printf("Could not unmarshal kinesis data for sequence %s, %v", *r.SequenceNumber, err)
					continue
				}

				url := filters.findUrl(msg)
				if url != "" {
					err = post(url, *r.SequenceNumber, r.Data)
					if err != nil {
						log.Print(err)
					}
				} else {
					persistSequenceNumberToDisk(*r.SequenceNumber)
				}

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

func post(url, sequence string, data []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))

	if err != nil {
		return fmt.Errorf("Error posting to %v for sequence number %s- %v", url, sequence, err)
	}
	if resp.StatusCode > 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Received status code %d when posting to %s for sequence number %s- %v", resp.StatusCode, url, sequence, err)
	}
	defer resp.Body.Close()
	return nil
}

func persistSequenceNumberToDisk(number string) {
	ioutil.WriteFile("current_sequence_number", []byte(number), 0644)
}

func getSequenceNumberFromDisk() string {
	number, _ := ioutil.ReadFile("current_sequence_number")
	return string(number)
}

func getIterator(client *kinesis.Kinesis, input *kinesis.GetShardIteratorInput) (*string, error) {
	sout, err := client.GetShardIterator(input)
	if err != nil {
		return nil, err
	}

	return sout.ShardIterator, nil
}

func getIteratorFromSequenceNumber(sequenceNumber string, client *kinesis.Kinesis) (*string, error) {
	sinput := &kinesis.GetShardIteratorInput{
		ShardId:                shardID,
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
		StartingSequenceNumber: aws.String(sequenceNumber),
		StreamName:             stream,
	}

	return getIterator(client, sinput)
}

func getIteratorFromNow(client *kinesis.Kinesis) (*string, error) {
	sinput := &kinesis.GetShardIteratorInput{
		ShardId:           shardID,
		ShardIteratorType: aws.String("AT_TIMESTAMP"),
		Timestamp:         aws.Time(time.Now()),
		StreamName:        stream,
	}

	return getIterator(client, sinput)
}
