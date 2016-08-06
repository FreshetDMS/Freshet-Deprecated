# DataFlowPipelineRunner

## fromOptions

* register IO factories
* option validations
* staging and temp location path validation
* setup file staging
* jobConfig name and project settings

## Constructor

* Setup transformations map (overrides map)

## apply

* Customize and transform PTransform's to work with Google Cloud Dataflow.
* Customizations for Combine.GroupedValues.class, GroupByKey.class, Window.Bound.class, Flatten.FlattenPCollectionList.class and any PTransform in overrides map.

## applyWindow

* Window transform customization

## run(Pipeline pipeline)

* stage files
* register debuggee based on options
* translator generates a cloud dataflow jobConfig spec from *pipeline*.
* set up cloud dataflow jobConfig environment (temp location, temp dataset, experiments)
* setup work harness container image?
* setup jobConfig requirements (env version, jobConfig type [streaming vs batch]
* configure jobConfig update if this is an update
* execute jobConfig via dataflow client.
* setup post launch monitoring