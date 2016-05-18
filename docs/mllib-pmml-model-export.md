---
layout: global
title: PMML model export - spark.mllib
displayTitle: PMML model export - spark.mllib
---

* Table of contents
{:toc}

## `spark.mllib` supported models

`spark.mllib` supports model export to Predictive Model Markup Language ([PMML](http://en.wikipedia.org/wiki/Predictive_Model_Markup_Language)).

The table below outlines the `spark.mllib` models that can be exported to PMML and their equivalent PMML model.

<table class="table">
  <thead>
    <tr><th>`spark.mllib` model</th><th>PMML model</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>KMeansModel</td><td>ClusteringModel</td>
    </tr>    
    <tr>
      <td>LinearRegressionModel</td><td>RegressionModel (functionName="regression")</td>
    </tr>
    <tr>
      <td>RidgeRegressionModel</td><td>RegressionModel (functionName="regression")</td>
    </tr>
    <tr>
      <td>LassoModel</td><td>RegressionModel (functionName="regression")</td>
    </tr>
    <tr>
      <td>SVMModel</td><td>RegressionModel (functionName="classification" normalizationMethod="none")</td>
    </tr>
    <tr>
      <td>Binary LogisticRegressionModel</td><td>RegressionModel (functionName="classification" normalizationMethod="logit")</td>
    </tr>
  </tbody>
</table>

## Examples
<div class="codetabs">

<div data-lang="scala" markdown="1">
To export a supported `model` (see table above) to PMML, simply call `model.toPMML`.

As well as exporting the PMML model to a String (`model.toPMML` as in the example above), you can export the PMML model to other formats.

Refer to the [`KMeans` Scala docs](api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) and [`Vectors` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors) for details on the API.

Here a complete example of building a KMeansModel and print it out in PMML format:
{% include_example scala/org/apache/spark/examples/mllib/PMMLModelExportExample.scala %}

For unsupported models, either you will not find a `.toPMML` method or an `IllegalArgumentException` will be thrown.

</div>

</div>
