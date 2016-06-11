# DataFlowPipelineRunner

## fromOptions

* register IO factories
* option validations
* staging and temp location path validation
* setup file staging
* job name and project settings

## Constructor

* Setup transformations map (overrides map)

## apply

* Customize and transform PTransform's to work with Google Cloud Dataflow.
* Customizations for Combine.GroupedValues.class, GroupByKey.class, Window.Bound.class, Flatten.FlattenPCollectionList.class and any PTransform in overrides map.

## applyWindow

* Window transform customization

## run(Pipeline pipeline)

