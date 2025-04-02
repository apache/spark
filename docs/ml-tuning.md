---
layout: global
title: "ML Tuning"
displayTitle: "ML Tuning: model selection and hyperparameter tuning"
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

`\[
\newcommand{\R}{\mathbb{R}}
\newcommand{\E}{\mathbb{E}}
\newcommand{\x}{\mathbf{x}}
\newcommand{\y}{\mathbf{y}}
\newcommand{\wv}{\mathbf{w}}
\newcommand{\av}{\mathbf{\alpha}}
\newcommand{\bv}{\mathbf{b}}
\newcommand{\N}{\mathbb{N}}
\newcommand{\id}{\mathbf{I}}
\newcommand{\ind}{\mathbf{1}}
\newcommand{\0}{\mathbf{0}}
\newcommand{\unit}{\mathbf{e}}
\newcommand{\one}{\mathbf{1}}
\newcommand{\zero}{\mathbf{0}}
\]`

This section describes how to use MLlib's tooling for tuning ML algorithms and Pipelines.
Built-in Cross-Validation and other tooling allow users to optimize hyperparameters in algorithms and Pipelines.

**Table of contents**

* This will become a table of contents (this text will be scraped).
{:toc}

# Model selection (a.k.a. hyperparameter tuning)

An important task in ML is *model selection*, or using data to find the best model or parameters for a given task.  This is also called *tuning*.
Tuning may be done for individual `Estimator`s such as `LogisticRegression`, or for entire `Pipeline`s which include multiple algorithms, featurization, and other steps.  Users can tune an entire `Pipeline` at once, rather than tuning each element in the `Pipeline` separately.

MLlib supports model selection using tools such as [`CrossValidator`](api/scala/org/apache/spark/ml/tuning/CrossValidator.html) and [`TrainValidationSplit`](api/scala/org/apache/spark/ml/tuning/TrainValidationSplit.html).
These tools require the following items:

* [`Estimator`](api/scala/org/apache/spark/ml/Estimator.html): algorithm or `Pipeline` to tune
* Set of `ParamMap`s: parameters to choose from, sometimes called a "parameter grid" to search over
* [`Evaluator`](api/scala/org/apache/spark/ml/evaluation/Evaluator.html): metric to measure how well a fitted `Model` does on held-out test data

At a high level, these model selection tools work as follows:

* They split the input data into separate training and test datasets.
* For each (training, test) pair, they iterate through the set of `ParamMap`s:
  * For each `ParamMap`, they fit the `Estimator` using those parameters, get the fitted `Model`, and evaluate the `Model`'s performance using the `Evaluator`.
* They select the `Model` produced by the best-performing set of parameters.

The `Evaluator` can be a [`RegressionEvaluator`](api/scala/org/apache/spark/ml/evaluation/RegressionEvaluator.html)
for regression problems, a [`BinaryClassificationEvaluator`](api/scala/org/apache/spark/ml/evaluation/BinaryClassificationEvaluator.html)
for binary data, a [`MulticlassClassificationEvaluator`](api/scala/org/apache/spark/ml/evaluation/MulticlassClassificationEvaluator.html)
for multiclass problems, a [`MultilabelClassificationEvaluator`](api/scala/org/apache/spark/ml/evaluation/MultilabelClassificationEvaluator.html)
 for multi-label classifications, or a
[`RankingEvaluator`](api/scala/org/apache/spark/ml/evaluation/RankingEvaluator.html) for ranking problems. The default metric used to
choose the best `ParamMap` can be overridden by the `setMetricName` method in each of these evaluators.

To help construct the parameter grid, users can use the [`ParamGridBuilder`](api/scala/org/apache/spark/ml/tuning/ParamGridBuilder.html) utility.
By default, sets of parameters from the parameter grid are evaluated in serial. Parameter evaluation can be done in parallel by setting `parallelism` with a value of 2 or more (a value of 1 will be serial) before running model selection with `CrossValidator` or `TrainValidationSplit`.
The value of `parallelism` should be chosen carefully to maximize parallelism without exceeding cluster resources, and larger values may not always lead to improved performance.  Generally speaking, a value up to 10 should be sufficient for most clusters.

# Cross-Validation

`CrossValidator` begins by splitting the dataset into a set of *folds* which are used as separate training and test datasets. E.g., with `$k=3$` folds, `CrossValidator` will generate 3 (training, test) dataset pairs, each of which uses 2/3 of the data for training and 1/3 for testing.  To evaluate a particular `ParamMap`, `CrossValidator` computes the average evaluation metric for the 3 `Model`s produced by fitting the `Estimator` on the 3 different (training, test) dataset pairs.

After identifying the best `ParamMap`, `CrossValidator` finally re-fits the `Estimator` using the best `ParamMap` and the entire dataset.

**Examples: model selection via cross-validation**

The following example demonstrates using `CrossValidator` to select from a grid of parameters.

Note that cross-validation over a grid of parameters is expensive.
E.g., in the example below, the parameter grid has 3 values for `hashingTF.numFeatures` and 2 values for `lr.regParam`, and `CrossValidator` uses 2 folds.  This multiplies out to `$(3 \times 2) \times 2 = 12$` different models being trained.
In realistic settings, it can be common to try many more parameters and use more folds (`$k=3$` and `$k=10$` are common).
In other words, using `CrossValidator` can be very expensive.
However, it is also a well-established method for choosing parameters which is more statistically sound than heuristic hand-tuning.

<div class="codetabs">

<div data-lang="python" markdown="1">

Refer to the [`CrossValidator` Python docs](api/python/reference/api/pyspark.ml.tuning.CrossValidator.html) for more details on the API.

{% include_example python/ml/cross_validator.py %}
</div>

<div data-lang="scala" markdown="1">

Refer to the [`CrossValidator` Scala docs](api/scala/org/apache/spark/ml/tuning/CrossValidator.html) for details on the API.

{% include_example scala/org/apache/spark/examples/ml/ModelSelectionViaCrossValidationExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [`CrossValidator` Java docs](api/java/org/apache/spark/ml/tuning/CrossValidator.html) for details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaModelSelectionViaCrossValidationExample.java %}
</div>

</div>

# Train-Validation Split

In addition to  `CrossValidator` Spark also offers `TrainValidationSplit` for hyper-parameter tuning.
`TrainValidationSplit` only evaluates each combination of parameters once, as opposed to k times in
 the case of `CrossValidator`. It is, therefore, less expensive,
 but will not produce as reliable results when the training dataset is not sufficiently large.

Unlike `CrossValidator`, `TrainValidationSplit` creates a single (training, test) dataset pair.
It splits the dataset into these two parts using the `trainRatio` parameter. For example with `$trainRatio=0.75$`,
`TrainValidationSplit` will generate a training and test dataset pair where 75% of the data is used for training and 25% for validation.

Like `CrossValidator`, `TrainValidationSplit` finally fits the `Estimator` using the best `ParamMap` and the entire dataset.

**Examples: model selection via train validation split**

<div class="codetabs">

<div data-lang="python" markdown="1">

Refer to the [`TrainValidationSplit` Python docs](api/python/reference/api/pyspark.ml.tuning.TrainValidationSplit.html) for more details on the API.

{% include_example python/ml/train_validation_split.py %}
</div>

<div data-lang="scala" markdown="1">

Refer to the [`TrainValidationSplit` Scala docs](api/scala/org/apache/spark/ml/tuning/TrainValidationSplit.html) for details on the API.

{% include_example scala/org/apache/spark/examples/ml/ModelSelectionViaTrainValidationSplitExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [`TrainValidationSplit` Java docs](api/java/org/apache/spark/ml/tuning/TrainValidationSplit.html) for details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaModelSelectionViaTrainValidationSplitExample.java %}
</div>

</div>
