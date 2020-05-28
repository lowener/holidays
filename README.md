# Holidays

## Description

This projet is a CSV parser realised in Scala.
It will read the data, and register them in an ElasticSearch database.
And you can do some query also! ;)

## Dependencies

There are two dependencies:
* elasticsearch
* scalatest

## How to

### ElasticSearch

* `apt-get install elasticsearch` for ubuntu
* `pacman -S elasticsearch` for archlinux
* Setup elastic search. The default configuration should work

### Launch

You need elasticsearch's service to be started, on archlinux:
`systemctl start elasticsearch.service`

`sbt "run airports.csv countries.csv runways.csv"`


## Features

* ElasticSearch database
* Text User Interface
* Input suggestions on fuzzy names  (e.g. "Frince" => "France")
* Unit tests in 'test/scala/holidays'
