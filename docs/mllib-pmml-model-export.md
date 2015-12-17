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

Refer to the [`KMeans` Scala docs](api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) and [`Vectors` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors) for details on the API.

Here a complete example of building a KMeansModel and print it out in PMML format:
{% highlight scala %}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/kmeans_data.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Export to PMML
println("PMML Model:\n" + clusters.toPMML)
{% endhighlight %}

As well as exporting the PMML model to a String (`model.toPMML` as in the example above), you can export the PMML model to other formats:

{% highlight scala %}
// Export the model to a String in PMML format
clusters.toPMML

// Export the model to a local file in PMML format
clusters.toPMML("/tmp/kmeans.xml")

// Export the model to a directory on a distributed file system in PMML format
clusters.toPMML(sc,"/tmp/kmeans")

// Export the model to the OutputStream in PMML format
clusters.toPMML(System.out)
{% endhighlight %}

For unsupported models, either you will not find a `.toPMML` method or an `IllegalArgumentException` will be thrown.

</div>

</div>
