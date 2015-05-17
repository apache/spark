---
layout: global
title: PMML model export - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - PMML model export
---

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
      <td>LogisticRegressionModel</td><td>RegressionModel</td>
    </tr>
    <tr>
      <td>SVMModel</td><td>RegressionModel</td>
    </tr>
  </tbody>
</table>
