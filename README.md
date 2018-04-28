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

* Unit tests in 'test/scala/holidays'
* ElasticSearch database
* Input suggestions on fuzzy names  (e.g. "Frince" => "France")
