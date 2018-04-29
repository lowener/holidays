# Holidays

## Dependencies

There are two dependencies:
* elasticsearch
* scalatest

## How to

### ElasticSearch

* `apt-get install elasticsearch`
* Setup elastic search. The default configuration should work

### Launch

`sbt "run airports.csv countries.csv runways.csv"`


## Features

* ElasticSearch database
* Text User Interface
* Input suggestions on fuzzy names  (e.g. "Frince" => "France")
* Unit tests in 'test/scala/holidays'


## Disclaimer

* According to the official ElasticSearch doc: "Document counts are approximative":
https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#search-aggregations-bucket-terms-aggregation-approximate-counts
