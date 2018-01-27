# A Bulk Data Export Client in Go

This is a project during the [Bulk Data Export](http://wiki.hl7.org/index.php?title=201801_Bulk_Data) track of the [FHIR Connectathon 17](http://wiki.hl7.org/index.php?title=FHIR_Connectathon_17).

To run this demo, first clone the project to your local drive
```
git clone https://github.com/toby-hu/test
```

Next, start a bulk data export enabled server for this client to communicate with. An example server is the [bulk-data-server](https://github.com/smart-on-fhir/bulk-data-server) provided by the Connectathon organizers. Once the server is brought up, set the `BASE_URL` variable to the server's endpoint, e.g.
```
BASE_URL='http://localhost:9443/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MX0/fhir'
```

For running the client code, make sure that Go language installed on your computer by following [the instructions from the golang.org site](https://golang.org/doc/install).

Then, run the client code using `go run` command, which requires a required `--url` flag and an optional `--output_prefix` flag. For example, for scenario 1 (full bulk data export, open endpoint):
```
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/patient/\$everything
```

One can also fetch filtered data in scenario 2 using
```
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/patient/\$everything?_type=Patient
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/Group/5/\$everything

```
