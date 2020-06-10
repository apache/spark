---
layout: global
title: PMML model export - RDD-based API
displayTitle: PMML model export - RDD-based API
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## spark.mllib supported models

`spark.mllib` supports model export to Predictive Model Markup Language ([PMML](http://en.wikipedia.org/wiki/Predictive_Model_Markup_Language)).

The table below outlines the `spark.mllib` models that can be exported to PMML and their equivalent PMML model.

<table class="table">
  <thead>
    <tr><th>spark.mllib model</th><th>PMML model</th></tr>
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

Refer to the [`KMeans` Scala docs](api/scala/org/apache/spark/mllib/clustering/KMeans.html) and [`Vectors` Scala docs](api/scala/org/apache/spark/mllib/linalg/Vectors$.html) for details on the API.

Here a complete example of building a KMeansModel and print it out in PMML format:
{% include_example scala/org/apache/spark/examples/mllib/PMMLModelExportExample.scala %}

For unsupported models, either you will not find a `.toPMML` method or an `IllegalArgumentException` will be thrown.

</div>

</div>
