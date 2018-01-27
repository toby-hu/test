# A Bulk Data Export Client in Go

This is a project during the [Bulk Data Export](http://wiki.hl7.org/index.php?title=201801_Bulk_Data) track of the [FHIR Connectathon 17](http://wiki.hl7.org/index.php?title=FHIR_Connectathon_17).

To run this demo, first clone the project to your local drive
```
git clone https://github.com/toby-hu/test
```

Then, run the client code using `go run` command, which requires a required `--url` flag and an optional `--output_prefix` flag. For example, for scenario 1 (full bulk data export, open endpoint):
```
BASE_URL='http://localhost:9443/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MX0/fhir'
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/patient/\$everything
```

One can also fetch filtered data in scenario 2 using
```
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/patient/\$everything?_type=Patient
go run main.go --output_prefix=tmp/ --url=${BASE_URL}/Group/5/\$everything

```
