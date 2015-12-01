---
layout: global
title: Decision Trees - SparkML
displayTitle: <a href="ml-guide.html">ML</a> - Decision Trees
---

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}


# Overview

[Decision trees](http://en.wikipedia.org/wiki/Decision_tree_learning)
and their ensembles are popular methods for the machine learning tasks of
classification and regression. Decision trees are widely used since they are easy to interpret,
handle categorical features, extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions. Tree ensemble
algorithms such as random forests and boosting are among the top performers for classification and
regression tasks.

MLlib supports decision trees for binary and multiclass classification and for regression,
using both continuous and categorical features. The implementation partitions data by rows,
allowing distributed training with millions or even billions of instances.

Users can find more information about the decision tree algorithm in the [MLlib Decision Tree guide](mllib-decision-tree.html).  In this section, we demonstrate the Pipelines API for Decision Trees.

The Pipelines API for Decision Trees offers a bit more functionality than the original API.  In particular, for classification, users can get the predicted probability of each class (a.k.a. class conditional probabilities).

Ensembles of trees (Random Forests and Gradient-Boosted Trees) are described in the [Ensembles guide](ml-ensembles.html).

# Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

## Input Columns

<table class="table">
  <thead>
    <tr>
      <th align="left">Param name</th>
      <th align="left">Type(s)</th>
      <th align="left">Default</th>
      <th align="left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>labelCol</td>
      <td>Double</td>
      <td>"label"</td>
      <td>Label to predict</td>
    </tr>
    <tr>
      <td>featuresCol</td>
      <td>Vector</td>
      <td>"features"</td>
      <td>Feature vector</td>
    </tr>
  </tbody>
</table>

## Output Columns

<table class="table">
  <thead>
    <tr>
      <th align="left">Param name</th>
      <th align="left">Type(s)</th>
      <th align="left">Default</th>
      <th align="left">Description</th>
      <th align="left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>predictionCol</td>
      <td>Double</td>
      <td>"prediction"</td>
      <td>Predicted label</td>
      <td></td>
    </tr>
    <tr>
      <td>rawPredictionCol</td>
      <td>Vector</td>
      <td>"rawPrediction"</td>
      <td>Vector of length # classes, with the counts of training instance labels at the tree node which makes the prediction</td>
      <td>Classification only</td>
    </tr>
    <tr>
      <td>probabilityCol</td>
      <td>Vector</td>
      <td>"probability"</td>
      <td>Vector of length # classes equal to rawPrediction normalized to a multinomial distribution</td>
      <td>Classification only</td>
    </tr>
  </tbody>
</table>

# Examples

The below examples demonstrate the Pipelines API for Decision Trees. The main differences between this API and the [original MLlib Decision Tree API](mllib-decision-tree.html) are:

* support for ML Pipelines
* separation of Decision Trees for classification vs. regression
* use of DataFrame metadata to distinguish continuous and categorical features


## Classification

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the Decision Tree algorithm can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/index.html#org.apache.spark.ml.classification.DecisionTreeClassifier).

{% include_example scala/org/apache/spark/examples/ml/DecisionTreeClassificationExample.scala %}

</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html).

{% include_example java/org/apache/spark/examples/ml/JavaDecisionTreeClassificationExample.java %}

</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier).

{% include_example python/ml/decision_tree_classification_example.py %}

</div>

</div>


## Regression

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use a feature transformer to index categorical features, adding metadata to the `DataFrame` which the Decision Tree algorithm can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/index.html#org.apache.spark.ml.regression.DecisionTreeRegressor).

{% include_example scala/org/apache/spark/examples/ml/DecisionTreeRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/regression/DecisionTreeRegressor.html).

{% include_example java/org/apache/spark/examples/ml/JavaDecisionTreeRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor).

{% include_example python/ml/decision_tree_regression_example.py %}
</div>

</div>
