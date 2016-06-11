# 06-11-2016

## TODO

* First define Samza specific input and output types (KafkaIO)
* Simple job which reads from one Kafka topic and write that message to other Kafka topics.

## Requirements

* Job package should be in the options
* Convert the pipeline to one or more Samza job(s)
* Pipeline is represented as JSON or in-memory DAG and then use JSON to describe the configuration (including PTransform's) of a single job

## Questions?

* Do we need coordinator system in options?
* May be all the systems in options and only get streams through KafkaIO like interface?
* How to distribute generated code for transforms to tasks? May be serialized objects in JSON
