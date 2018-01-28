# A Bulk Data Export Client in Go

This is a project during the [Bulk Data Export](http://wiki.hl7.org/index.php?title=201801_Bulk_Data)
track of the [FHIR Connectathon 17](http://wiki.hl7.org/index.php?title=FHIR_Connectathon_17).

To run this demo, first clone the project to your local drive
```
git clone https://github.com/toby-hu/test
cd test/client/
```

Next, start a bulk data export enabled server for this client to communicate with. An example server is the
[bulk-data-server](https://github.com/smart-on-fhir/bulk-data-server)
provided by the Connectathon organizers. Once the server is brought up,
set the `BASE_URL` variable to the server's endpoint, e.g.
```
BASE_URL='http://localhost:9443/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MX0/fhir'
```

For running the client code, make sure that Go language installed on your
computer by following
[the instructions from the golang.org site](https://golang.org/doc/install).
This version of the client has the capability of to Google Cloud for
testing exporting data to Google Cloud Storage and loading to BigQuery
(enabled by flags), and requires `cloud.google.com/go/bigquery`,
`cloud.google.com/go/storage`, `golang.org/x/net/context` libraries.
Install any missing library following
[the instruction](https://cloud.google.com/go/home) using `go get`
command, for example:
```
go get -u cloud.google.com/go/bigquery
go get -u cloud.google.com/go/storage
go get -u golang.org/x/net/context
```
until `go build main.go` does not report any missing library error.

To run the client code, either use the compiled `main` binary from the
the previous step, or use `go run` command. Both methods require a `--url`
flag, and depending on the data destination, the following flags can be
provided.

## Dumping Fetched Resources to Files
To save the fetched files to files, provide and an `--output_prefix` flag
to specify where the files should be output to.
For example, for scenario 1 (full bulk data export, open endpoint):
```
mkdir tmp
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/patient/\$everything?output-format=application/fhir%2Bndjson
```

One can also fetch filtered data in scenario 2 using
```
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/patient/\$everything?_type=Patient&output-format=application/fhir%2Bndjson
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/Group/5/\$everything?output-format=application/fhir%2Bndjson

```
Once the execution succeeds, the fetched `.ndjson` files can be found in
the `output_prefix` directory.

## Exporting Fetched Resources to Google Cloud Storage
To export the `ndjson` files to GCS, create a Google Cloud project and a
a Google Cloud Storage Bucket by following the
[getting started document](https://cloud.google.com/storage/docs/quickstart-console).
The project and bucket names will later be used for the `gc_project_id`
and `gcs_bucket_name` flags.

Next, ensure that the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable contains the path to your downloaded JSON account key by
following the [Application Default Credentials setup guide](https://cloud.google.com/docs/authentication/getting-started).

Now you are ready to use the client to export the bulk data
fetched from the server directly to the specified GCS bucket.
```
go run main.go --url=${BASE_URL}/Patient/\$everything?output-format=application/fhir%2Bndjson --gc_project_id=<project_id> --gcs_bucket_name=<bucket_name>
```

## Loading Data to BigQuery
To further load the data from GCS to BigQuery, ensure that a dataset is created in the [BigQuery console](https://bigquery.cloud.google.com), and run the command with an additional `bq_dataset_name` flag, e.g.
```
go run main.go --url=${BASE_URL}/Patient/\$everything?output-format=application/fhir%2Bndjson --gc_project_id=<project_id> --gcs_bucket_name=<bucket_name> --bq_dataset_name=<dataset_name>
```

## Other Commandline Flags

*  `links_in_body`: during the connectathon, a decision was made to
switch the bulk downloading links from the status query response header
to body. This flag defaults to `true` for the revised behavior, but can
be set to `false` for clients supporting the deprecated behavior.
*  `retry_sleep_secs`: number of seconds between status query retries, defaults to 5.