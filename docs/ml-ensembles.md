---
layout: global
title: Ensembles
displayTitle: <a href="ml-guide.html">ML</a> - Ensembles
---

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

An [ensemble method](http://en.wikipedia.org/wiki/Ensemble_learning)
is a learning algorithm which creates a model composed of a set of other base models.

## Tree Ensembles

The Pipelines API supports two major tree ensemble algorithms: [Random Forests](http://en.wikipedia.org/wiki/Random_forest) and [Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting).
Both use [MLlib decision trees](ml-decision-tree.html) as their base models.

Users can find more information about ensemble algorithms in the [MLlib Ensemble guide](mllib-ensembles.html).  In this section, we demonstrate the Pipelines API for ensembles.

The main differences between this API and the [original MLlib ensembles API](mllib-ensembles.html) are:
* support for ML Pipelines
* separation of classification vs. regression
* use of DataFrame metadata to distinguish continuous and categorical features
* a bit more functionality for random forests: estimates of feature importance, as well as the predicted probability of each class (a.k.a. class conditional probabilities) for classification.

### Random Forests

[Random forests](http://en.wikipedia.org/wiki/Random_forest)
are ensembles of [decision trees](ml-decision-tree.html).
Random forests combine many decision trees in order to reduce the risk of overfitting.
MLlib supports random forests for binary and multiclass classification and for regression,
using both continuous and categorical features.

This section gives examples of using random forests with the Pipelines API.
For more information on the algorithm, please see the [main MLlib docs on random forests](mllib-ensembles.html).

#### Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

##### Input Columns

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

##### Output Columns (Predictions)

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

#### Example: Classification

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.classification.RandomForestClassifier) for more details.

{% include_example scala/org/apache/spark/examples/ml/RandomForestClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/RandomForestClassifier.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaRandomForestClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.classification.RandomForestClassifier) for more details.

{% include_example python/ml/random_forest_classifier_example.py %}
</div>
</div>

#### Example: Regression

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use a feature transformer to index categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.regression.RandomForestRegressor) for more details.

{% include_example scala/org/apache/spark/examples/ml/RandomForestRegressorExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/RandomForestRegressor.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaRandomForestRegressorExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.regression.RandomForestRegressor) for more details.

{% include_example python/ml/random_forest_regressor_example.py %}
</div>
</div>

### Gradient-Boosted Trees (GBTs)

[Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting)
are ensembles of [decision trees](ml-decision-tree.html).
GBTs iteratively train decision trees in order to minimize a loss function.
MLlib supports GBTs for binary classification and for regression,
using both continuous and categorical features.

This section gives examples of using GBTs with the Pipelines API.
For more information on the algorithm, please see the [main MLlib docs on GBTs](mllib-ensembles.html).

#### Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

##### Input Columns

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

Note that `GBTClassifier` currently only supports binary labels.

##### Output Columns (Predictions)

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
  </tbody>
</table>

In the future, `GBTClassifier` will also output columns for `rawPrediction` and `probability`, just as `RandomForestClassifier` does.

#### Example: Classification

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.classification.GBTClassifier) for more details.

{% include_example scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/GBTClassifier.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaGradientBoostedTreeClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.classification.GBTClassifier) for more details.

{% include_example python/ml/gradient_boosted_tree_classifier_example.py %}
</div>
</div>

#### Example: Regression

Note: For this example dataset, `GBTRegressor` actually only needs 1 iteration, but that will not
be true in general.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.regression.GBTRegressor) for more details.

{% include_example scala/org/apache/spark/examples/ml/GradientBoostedTreeRegressorExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/GBTRegressor.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaGradientBoostedTreeRegressorExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/pyspark.ml.html#pyspark.ml.regression.GBTRegressor) for more details.

{% include_example python/ml/gradient_boosted_tree_regressor_example.py %}
</div>
</div>


## One-vs-Rest (a.k.a. One-vs-All)

[OneVsRest](http://en.wikipedia.org/wiki/Multiclass_classification#One-vs.-rest) is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently.  It is also known as "One-vs-All."

`OneVsRest` is implemented as an `Estimator`. For the base classifier it takes instances of `Classifier` and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.

Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.

### Example

The example below demonstrates how to load the
[Iris dataset](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale), parse it as a DataFrame and perform multiclass classification using `OneVsRest`. The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/index.html#org.apache.spark.ml.classifier.OneVsRest) for more details.

{% include_example scala/org/apache/spark/examples/ml/OneVsRestExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/OneVsRest.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaOneVsRestExample.java %}
</div>
</div>
