# 07-22-2016

* We will get the job package from the options. It can be a HTTP url, HDFS url or local file depending on the job runner mode. The job will fail if job can't find the package in the proper location. 
* Translator should keep track of output streams (Kafka topics) as done in Dataflow runner's translator. So we can monitor running jobs. (```  private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;```)
* Need to implement a Pipeline.Visitor that visit pipeline and generate one or more SamzaJob's according to the transform logic
* Dataflow translators's context is a good way to keep track of the progress of pipeline translation

## Beam Transforms

* **Create:** takes a collection of elements (of type ```T```) and returns a ```PCollection<T>``` containing elements. ```Create.of``` take some inputs and returns a ```Values``` object which will returns the ```PColleciton<T>``` once a input it applied.
* **Read:** ```PTransform``` for reading from a source (bounded or unbounded).
* **ParDo.Bound:** Core element-wise transform in Beam. Invokes a user specified function on each of the elements in the input ```PCollection``` to produce zero or more elements. Similar to 'Mapper' or 'Reducer' in MapReduce. Often executed distributed. Has side inputs and side outputs. ```DoFn``` is serializable and distributed across workers. In this context Bound means there is a ```DoFn``` associated with ParDo transform.
* **TextIO:** transform for reading and writing text files
* **Write:** transform that writes to a ```Sink```.
* **Window:** logically divides up or groups the elements of a ```PCollection``` into finite windows according to ```WindowFn```.
* **GroupByKey:** takes ```KV``` ```PCollection``` and groups the value by key and window
* **Combine:** combine elements globally per-key
* **Flatten:** Converts ```PCollectionList``` to ```PCollection```. Like flatMap without map part.
* **ParDo.BoundMulti:** Similar to ```ParDo.Bound```, but output is of type ```PCollectionTuple```. And can emit to main and side outputs which are bundled into ```PCollectionTuple``` 

# 06-11-2016

## TODO

* First define Samza specific input and output types (KafkaIO)
* Simple jobConfig which reads from one Kafka topic and write that message to other Kafka topics.

## Requirements

* Job package should be in the options
* Convert the pipeline to one or more Samza jobConfig(s)
* Pipeline is represented as JSON or in-memory DAG and then use JSON to describe the configuration (including PTransform's) of a single jobConfig

## Questions?

* Do we need coordinator system in options?
* May be all the systems in options and only get streams through KafkaIO like interface?
* How to distribute generated code for transforms to tasks? May be serialized objects in JSON
