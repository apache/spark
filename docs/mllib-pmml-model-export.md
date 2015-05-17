---
layout: global
title: PMML model export - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - PMML model export
---

* Table of contents
{:toc}

## MLlib supported models

MLlib supports model export to Predictive Model Markup Language ([PMML](http://en.wikipedia.org/wiki/Predictive_Model_Markup_Language)) format.

The table below outlines the MLlib models that can be exported to PMML and their equivalent PMML format.

<table class="table">
  <thead>
    <tr><th>MLlib model</th><th>PMML model</th></tr>
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
      <td>LogisticRegressionModel</td><td>RegressionModel (functionName="classification" normalizationMethod="logit")</td>
    </tr>
  </tbody>
</table>

## Example: exporting KMeansModel
Same applies to other models...

## Example: exporting a model to String, file ...
