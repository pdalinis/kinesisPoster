# kinesisPoster

Read from a Kinesis stream and perform http posts conditionally based on the data.


## Assumptions

1. The Kinesis data is in json format.
1. The data contains a root key/value to trigger a post on.

## Setup

1. Verify that the ~/.aws/credentials default profile or the IAM Role of the machine has the following Kinesis permissions:
 1. GetRecords
 1. GetShardIterator
1. Edit the filters.json file and change it to support your Kinesis data with:
 1. The json key to look for
 1. The json value to match on
 1. The URL to post the data to

## Logging

The logger writes all output to stderr.
