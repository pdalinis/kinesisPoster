package main

import (
	"flag"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var stream = flag.String("stream", "", "The name of the Kinesis stream to read from")
var region = flag.String("region", "us-west-2", "The aws region")

func main() {
	flag.Parse()
	if *stream == "" {
		panic("stream is required")
	}

	client := kinesis.New(session.New(), &aws.Config{Region: aws.String(*region)})

	req := &kinesis.PutRecordInput{
		StreamName:   stream,
		PartitionKey: aws.String("1"),
	}

	data1 := []byte(`{"myKey": "myValue", "another": "keyset"}`)
	data2 := []byte(`{"myKey": "otherValue", "foo": "bar"}`)
	data3 := []byte(`{"someKey": "someValue", "another": "keyset"}`)

	for {

		req.Data = data1
		client.PutRecord(req)
		time.Sleep(100 * time.Millisecond)

		req.Data = data2
		client.PutRecord(req)
		time.Sleep(100 * time.Millisecond)

		req.Data = data3
		client.PutRecord(req)
		time.Sleep(100 * time.Millisecond)
	}

}
