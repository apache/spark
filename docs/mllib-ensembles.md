---
layout: global
title: Ensembles - RDD-based API
displayTitle: Ensembles - RDD-based API
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

An [ensemble method](http://en.wikipedia.org/wiki/Ensemble_learning)
is a learning algorithm which creates a model composed of a set of other base models.
`spark.mllib` supports two major ensemble algorithms: [`GradientBoostedTrees`](api/scala/org/apache/spark/mllib/tree/GradientBoostedTrees.html) and [`RandomForest`](api/scala/org/apache/spark/mllib/tree/RandomForest$.html).
Both use [decision trees](mllib-decision-tree.html) as their base models.

## Gradient-Boosted Trees vs. Random Forests

Both [Gradient-Boosted Trees (GBTs)](mllib-ensembles.html#Gradient-Boosted-Trees-(GBTS)) and [Random Forests](mllib-ensembles.html#Random-Forests) are algorithms for learning ensembles of trees, but the training processes are different.  There are several practical trade-offs:

 * GBTs train one tree at a time, so they can take longer to train than random forests.  Random Forests can train multiple trees in parallel.
   * On the other hand, it is often reasonable to use smaller (shallower) trees with GBTs than with Random Forests, and training smaller trees takes less time.
 * Random Forests can be less prone to overfitting.  Training more trees in a Random Forest reduces the likelihood of overfitting, but training more trees with GBTs increases the likelihood of overfitting.  (In statistical language, Random Forests reduce variance by using more trees, whereas GBTs reduce bias by using more trees.)
 * Random Forests can be easier to tune since performance improves monotonically with the number of trees (whereas performance can start to decrease for GBTs if the number of trees grows too large).

In short, both algorithms can be effective, and the choice should be based on the particular dataset.

## Random Forests

[Random forests](http://en.wikipedia.org/wiki/Random_forest)
are ensembles of [decision trees](mllib-decision-tree.html).
Random forests are one of the most successful machine learning models for classification and
regression.  They combine many decision trees in order to reduce the risk of overfitting.
Like decision trees, random forests handle categorical features,
extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions.

`spark.mllib` supports random forests for binary and multiclass classification and for regression,
using both continuous and categorical features.
`spark.mllib` implements random forests using the existing [decision tree](mllib-decision-tree.html)
implementation.  Please see the decision tree guide for more information on trees.

### Basic algorithm

Random forests train a set of decision trees separately, so the training can be done in parallel.
The algorithm injects randomness into the training process so that each decision tree is a bit
different.  Combining the predictions from each tree reduces the variance of the predictions,
improving the performance on test data.

#### Training

The randomness injected into the training process includes:

* Subsampling the original dataset on each iteration to get a different training set (a.k.a. bootstrapping).
* Considering different random subsets of features to split on at each tree node.

Apart from these randomizations, decision tree training is done in the same way as for individual decision trees.

#### Prediction

To make a prediction on a new instance, a random forest must aggregate the predictions from its set of decision trees.  This aggregation is done differently for classification and regression.

*Classification*: Majority vote. Each tree's prediction is counted as a vote for one class.  The label is predicted to be the class which receives the most votes.

*Regression*: Averaging. Each tree predicts a real value.  The label is predicted to be the average of the tree predictions.

### Usage tips

We include a few guidelines for using random forests by discussing the various parameters.
We omit some decision tree parameters since those are covered in the [decision tree guide](mllib-decision-tree.html).

The first two parameters we mention are the most important, and tuning them can often improve performance:

* **`numTrees`**: Number of trees in the forest.
  * Increasing the number of trees will decrease the variance in predictions, improving the model's test-time accuracy.
  * Training time increases roughly linearly in the number of trees.

* **`maxDepth`**: Maximum depth of each tree in the forest.
  * Increasing the depth makes the model more expressive and powerful.  However, deep trees take longer to train and are also more prone to overfitting.
  * In general, it is acceptable to train deeper trees when using random forests than when using a single decision tree.  One tree is more likely to overfit than a random forest (because of the variance reduction from averaging multiple trees in the forest).

The next two parameters generally do not require tuning.  However, they can be tuned to speed up training.

* **`subsamplingRate`**: This parameter specifies the size of the dataset used for training each tree in the forest, as a fraction of the size of the original dataset.  The default (1.0) is recommended, but decreasing this fraction can speed up training.

* **`featureSubsetStrategy`**: Number of features to use as candidates for splitting at each tree node.  The number is specified as a fraction or function of the total number of features.  Decreasing this number will speed up training, but can sometimes impact performance if too low.

### Examples

#### Classification

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform classification using a Random Forest.
The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`RandomForest` Scala docs](api/scala/org/apache/spark/mllib/tree/RandomForest$.html) and [`RandomForestModel` Scala docs](api/scala/org/apache/spark/mllib/tree/model/RandomForestModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/RandomForestClassificationExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`RandomForest` Java docs](api/java/org/apache/spark/mllib/tree/RandomForest.html) and [`RandomForestModel` Java docs](api/java/org/apache/spark/mllib/tree/model/RandomForestModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaRandomForestClassificationExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`RandomForest` Python docs](api/python/reference/api/pyspark.mllib.tree.RandomForest.html) and [`RandomForest` Python docs](api/python/reference/api/pyspark.mllib.tree.RandomForestModel.html) for more details on the API.

{% include_example python/mllib/random_forest_classification_example.py %}
</div>

</div>

#### Regression

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform regression using a Random Forest.
The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`RandomForest` Scala docs](api/scala/org/apache/spark/mllib/tree/RandomForest$.html) and [`RandomForestModel` Scala docs](api/scala/org/apache/spark/mllib/tree/model/RandomForestModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/RandomForestRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`RandomForest` Java docs](api/java/org/apache/spark/mllib/tree/RandomForest.html) and [`RandomForestModel` Java docs](api/java/org/apache/spark/mllib/tree/model/RandomForestModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaRandomForestRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`RandomForest` Python docs](api/python/reference/api/pyspark.mllib.tree.RandomForest.html) and [`RandomForest` Python docs](api/python/reference/api/pyspark.mllib.tree.RandomForestModel.html) for more details on the API.

{% include_example python/mllib/random_forest_regression_example.py %}
</div>

</div>

## Gradient-Boosted Trees (GBTs)

[Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting)
are ensembles of [decision trees](mllib-decision-tree.html).
GBTs iteratively train decision trees in order to minimize a loss function.
Like decision trees, GBTs handle categorical features,
extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions.

`spark.mllib` supports GBTs for binary classification and for regression,
using both continuous and categorical features.
`spark.mllib` implements GBTs using the existing [decision tree](mllib-decision-tree.html) implementation.  Please see the decision tree guide for more information on trees.

*Note*: GBTs do not yet support multiclass classification.  For multiclass problems, please use
[decision trees](mllib-decision-tree.html) or [Random Forests](mllib-ensembles.html#Random-Forest).

### Basic algorithm

Gradient boosting iteratively trains a sequence of decision trees.
On each iteration, the algorithm uses the current ensemble to predict the label of each training instance and then compares the prediction with the true label.  The dataset is re-labeled to put more emphasis on training instances with poor predictions.  Thus, in the next iteration, the decision tree will help correct for previous mistakes.

The specific mechanism for re-labeling instances is defined by a loss function (discussed below).  With each iteration, GBTs further reduce this loss function on the training data.

#### Losses

The table below lists the losses currently supported by GBTs in `spark.mllib`.
Note that each loss is applicable to one of classification or regression, not both.

Notation: $N$ = number of instances. $y_i$ = label of instance $i$.  $x_i$ = features of instance $i$.  $F(x_i)$ = model's predicted label for instance $i$.

<table class="table">
  <thead>
    <tr><th>Loss</th><th>Task</th><th>Formula</th><th>Description</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Log Loss</td>
	  <td>Classification</td>
	  <td>$2 \sum_{i=1}^{N} \log(1+\exp(-2 y_i F(x_i)))$</td><td>Twice binomial negative log likelihood.</td>
    </tr>
    <tr>
      <td>Squared Error</td>
	  <td>Regression</td>
	  <td>$\sum_{i=1}^{N} (y_i - F(x_i))^2$</td><td>Also called L2 loss.  Default loss for regression tasks.</td>
    </tr>
    <tr>
      <td>Absolute Error</td>
	  <td>Regression</td>
     <td>$\sum_{i=1}^{N} |y_i - F(x_i)|$</td><td>Also called L1 loss.  Can be more robust to outliers than Squared Error.</td>
    </tr>
  </tbody>
</table>

### Usage tips

We include a few guidelines for using GBTs by discussing the various parameters.
We omit some decision tree parameters since those are covered in the [decision tree guide](mllib-decision-tree.html).

* **`loss`**: See the section above for information on losses and their applicability to tasks (classification vs. regression).  Different losses can give significantly different results, depending on the dataset.

* **`numIterations`**: This sets the number of trees in the ensemble.  Each iteration produces one tree.  Increasing this number makes the model more expressive, improving training data accuracy.  However, test-time accuracy may suffer if this is too large.

* **`learningRate`**: This parameter should not need to be tuned.  If the algorithm behavior seems unstable, decreasing this value may improve stability.

* **`algo`**: The algorithm or task (classification vs. regression) is set using the tree [Strategy] parameter.

#### Validation while training

Gradient boosting can overfit when trained with more trees. In order to prevent overfitting, it is useful to validate while
training. The method runWithValidation has been provided to make use of this option. It takes a pair of RDD's as arguments, the
first one being the training dataset and the second being the validation dataset.

The training is stopped when the improvement in the validation error is not more than a certain tolerance
(supplied by the `validationTol` argument in `BoostingStrategy`). In practice, the validation error
decreases initially and later increases. There might be cases in which the validation error does not change monotonically,
and the user is advised to set a large enough negative tolerance and examine the validation curve using `evaluateEachIteration`
(which gives the error or loss per iteration) to tune the number of iterations.

### Examples

#### Classification

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform classification using Gradient-Boosted Trees with log loss.
The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`GradientBoostedTrees` Scala docs](api/scala/org/apache/spark/mllib/tree/GradientBoostedTrees.html) and [`GradientBoostedTreesModel` Scala docs](api/scala/org/apache/spark/mllib/tree/model/GradientBoostedTreesModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/GradientBoostingClassificationExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`GradientBoostedTrees` Java docs](api/java/org/apache/spark/mllib/tree/GradientBoostedTrees.html) and [`GradientBoostedTreesModel` Java docs](api/java/org/apache/spark/mllib/tree/model/GradientBoostedTreesModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaGradientBoostingClassificationExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`GradientBoostedTrees` Python docs](api/python/reference/api/pyspark.mllib.tree.GradientBoostedTrees.html) and [`GradientBoostedTreesModel` Python docs](api/python/reference/api/pyspark.mllib.tree.GradientBoostedTreesModel.html) for more details on the API.

{% include_example python/mllib/gradient_boosting_classification_example.py %}
</div>

</div>

#### Regression

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform regression using Gradient-Boosted Trees with Squared Error as the loss.
The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`GradientBoostedTrees` Scala docs](api/scala/org/apache/spark/mllib/tree/GradientBoostedTrees.html) and [`GradientBoostedTreesModel` Scala docs](api/scala/org/apache/spark/mllib/tree/model/GradientBoostedTreesModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/GradientBoostingRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`GradientBoostedTrees` Java docs](api/java/org/apache/spark/mllib/tree/GradientBoostedTrees.html) and [`GradientBoostedTreesModel` Java docs](api/java/org/apache/spark/mllib/tree/model/GradientBoostedTreesModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaGradientBoostingRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`GradientBoostedTrees` Python docs](api/python/reference/api/pyspark.mllib.tree.GradientBoostedTrees.html) and [`GradientBoostedTreesModel` Python docs](api/python/reference/api/pyspark.mllib.tree.GradientBoostedTreesModel.html) for more details on the API.

{% include_example python/mllib/gradient_boosting_regression_example.py %}
</div>

</div>
