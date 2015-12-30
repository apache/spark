---
layout: global
title: Classification and regression - spark.ml
displayTitle: Classification and regression - spark.ml
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

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

In `spark.ml`, we implement popular linear methods such as logistic
regression and linear least squares with $L_1$ or $L_2$ regularization.
Refer to [the linear methods in mllib](mllib-linear-methods.html) for
details about implementation and tuning.  We also include a DataFrame API for [Elastic
net](http://en.wikipedia.org/wiki/Elastic_net_regularization), a hybrid
of $L_1$ and $L_2$ regularization proposed in [Zou et al, Regularization
and variable selection via the elastic
net](http://users.stat.umn.edu/~zouxx019/Papers/elasticnet.pdf).
Mathematically, it is defined as a convex combination of the $L_1$ and
the $L_2$ regularization terms:
`\[
\alpha \left( \lambda \|\wv\|_1 \right) + (1-\alpha) \left( \frac{\lambda}{2}\|\wv\|_2^2 \right) , \alpha \in [0, 1], \lambda \geq 0
\]`
By setting $\alpha$ properly, elastic net contains both $L_1$ and $L_2$
regularization as special cases. For example, if a [linear
regression](https://en.wikipedia.org/wiki/Linear_regression) model is
trained with the elastic net parameter $\alpha$ set to $1$, it is
equivalent to a
[Lasso](http://en.wikipedia.org/wiki/Least_squares#Lasso_method) model.
On the other hand, if $\alpha$ is set to $0$, the trained model reduces
to a [ridge
regression](http://en.wikipedia.org/wiki/Tikhonov_regularization) model.
We implement Pipelines API for both linear regression and logistic
regression with elastic net regularization.


# Classification

## Logistic regression

Logistic regression is a popular method to predict a binary response. It is a special case of [Generalized Linear models](https://en.wikipedia.org/wiki/Generalized_linear_model) that predicts the probability of the outcome.
For more background and more details about the implementation, refer to the documentation of the [logistic regression in `spark.mllib`](mllib-linear-methods.html#logistic-regression). 

  > The current implementation of logistic regression in `spark.ml` only supports binary classes. Support for multiclass regression will be added in the future.

**Example**

The following example shows how to train a logistic regression model
with elastic net regularization. `elasticNetParam` corresponds to
$\alpha$ and `regParam` corresponds to $\lambda$.

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/LogisticRegressionWithElasticNetExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaLogisticRegressionWithElasticNetExample.java %}
</div>

<div data-lang="python" markdown="1">
{% include_example python/ml/logistic_regression_with_elastic_net.py %}
</div>

</div>

The `spark.ml` implementation of logistic regression also supports
extracting a summary of the model over the training set. Note that the
predictions and metrics which are stored as `DataFrame` in
`BinaryLogisticRegressionSummary` are annotated `@transient` and hence
only available on the driver.

<div class="codetabs">

<div data-lang="scala" markdown="1">

[`LogisticRegressionTrainingSummary`](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegressionTrainingSummary)
provides a summary for a
[`LogisticRegressionModel`](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegressionModel).
Currently, only binary classification is supported and the
summary must be explicitly cast to
[`BinaryLogisticRegressionTrainingSummary`](api/scala/index.html#org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary).
This will likely change when multiclass classification is supported.

Continuing the earlier example:

{% include_example scala/org/apache/spark/examples/ml/LogisticRegressionSummaryExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`LogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/LogisticRegressionTrainingSummary.html)
provides a summary for a
[`LogisticRegressionModel`](api/java/org/apache/spark/ml/classification/LogisticRegressionModel.html).
Currently, only binary classification is supported and the
summary must be explicitly cast to
[`BinaryLogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/BinaryLogisticRegressionTrainingSummary.html).
This will likely change when multiclass classification is supported.

Continuing the earlier example:

{% include_example java/org/apache/spark/examples/ml/JavaLogisticRegressionSummaryExample.java %}
</div>

<!--- TODO: Add python model summaries once implemented -->
<div data-lang="python" markdown="1">
Logistic regression model summary is not yet supported in Python.
</div>

</div>


## Decision tree classifier

Decision trees are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on decision trees](#decision-trees).

**Example**

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

## Random forest classifier

Random forests are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on random forests](#random-forests).

**Example**

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

## Gradient-boosted tree classifier

Gradient-boosted trees (GBTs) are a popular classification and regression method using ensembles of decision trees. 
More information about the `spark.ml` implementation can be found further in the [section on GBTs](#gradient-boosted-trees-gbts).

**Example**

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

## Multilayer perceptron classifier

Multilayer perceptron classifier (MLPC) is a classifier based on the [feedforward artificial neural network](https://en.wikipedia.org/wiki/Feedforward_neural_network). 
MLPC consists of multiple layers of nodes. 
Each layer is fully connected to the next layer in the network. Nodes in the input layer represent the input data. All other nodes maps inputs to the outputs 
by performing linear combination of the inputs with the node's weights `$\wv$` and bias `$\bv$` and applying an activation function. 
It can be written in matrix form for MLPC with `$K+1$` layers as follows:
`\[
\mathrm{y}(\x) = \mathrm{f_K}(...\mathrm{f_2}(\wv_2^T\mathrm{f_1}(\wv_1^T \x+b_1)+b_2)...+b_K)
\]`
Nodes in intermediate layers use sigmoid (logistic) function:
`\[
\mathrm{f}(z_i) = \frac{1}{1 + e^{-z_i}}
\]`
Nodes in the output layer use softmax function:
`\[
\mathrm{f}(z_i) = \frac{e^{z_i}}{\sum_{k=1}^N e^{z_k}}
\]`
The number of nodes `$N$` in the output layer corresponds to the number of classes. 

MLPC employes backpropagation for learning the model. We use logistic loss function for optimization and L-BFGS as optimization routine.

**Example**

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/MultilayerPerceptronClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaMultilayerPerceptronClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">
{% include_example python/ml/multilayer_perceptron_classification.py %}
</div>

</div>


## One-vs-Rest classifier (a.k.a. One-vs-All)

[OneVsRest](http://en.wikipedia.org/wiki/Multiclass_classification#One-vs.-rest) is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently.  It is also known as "One-vs-All."

`OneVsRest` is implemented as an `Estimator`. For the base classifier it takes instances of `Classifier` and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.

Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.

**Example**

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


# Regression

## Linear regression

The interface for working with linear regression models and model
summaries is similar to the logistic regression case.

**Example**

The following
example demonstrates training an elastic net regularized linear
regression model and extracting model summary statistics.

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/LinearRegressionWithElasticNetExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaLinearRegressionWithElasticNetExample.java %}
</div>

<div data-lang="python" markdown="1">
<!--- TODO: Add python model summaries once implemented -->
{% include_example python/ml/linear_regression_with_elastic_net.py %}
</div>

</div>


## Decision tree regression

Decision trees are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on decision trees](#decision-trees).

**Example**

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


## Random forest regression

Random forests are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on random forests](#random-forests).

**Example**

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

## Gradient-boosted tree regression

Gradient-boosted trees (GBTs) are a popular regression method using ensembles of decision trees. 
More information about the `spark.ml` implementation can be found further in the [section on GBTs](#gradient-boosted-trees-gbts).

**Example**

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


## Survival regression


In `spark.ml`, we implement the [Accelerated failure time (AFT)](https://en.wikipedia.org/wiki/Accelerated_failure_time_model) 
model which is a parametric survival regression model for censored data. 
It describes a model for the log of survival time, so it's often called 
log-linear model for survival analysis. Different from 
[Proportional hazards](https://en.wikipedia.org/wiki/Proportional_hazards_model) model
designed for the same purpose, the AFT model is more easily to parallelize 
because each instance contribute to the objective function independently.

Given the values of the covariates $x^{'}$, for random lifetime $t_{i}$ of 
subjects i = 1, ..., n, with possible right-censoring, 
the likelihood function under the AFT model is given as:
`\[
L(\beta,\sigma)=\prod_{i=1}^n[\frac{1}{\sigma}f_{0}(\frac{\log{t_{i}}-x^{'}\beta}{\sigma})]^{\delta_{i}}S_{0}(\frac{\log{t_{i}}-x^{'}\beta}{\sigma})^{1-\delta_{i}}
\]`
Where $\delta_{i}$ is the indicator of the event has occurred i.e. uncensored or not.
Using $\epsilon_{i}=\frac{\log{t_{i}}-x^{'}\beta}{\sigma}$, the log-likelihood function
assumes the form:
`\[
\iota(\beta,\sigma)=\sum_{i=1}^{n}[-\delta_{i}\log\sigma+\delta_{i}\log{f_{0}}(\epsilon_{i})+(1-\delta_{i})\log{S_{0}(\epsilon_{i})}]
\]`
Where $S_{0}(\epsilon_{i})$ is the baseline survivor function,
and $f_{0}(\epsilon_{i})$ is corresponding density function.

The most commonly used AFT model is based on the Weibull distribution of the survival time. 
The Weibull distribution for lifetime corresponding to extreme value distribution for 
log of the lifetime, and the $S_{0}(\epsilon)$ function is:
`\[   
S_{0}(\epsilon_{i})=\exp(-e^{\epsilon_{i}})
\]`
the $f_{0}(\epsilon_{i})$ function is:
`\[
f_{0}(\epsilon_{i})=e^{\epsilon_{i}}\exp(-e^{\epsilon_{i}})
\]`
The log-likelihood function for AFT model with Weibull distribution of lifetime is:
`\[
\iota(\beta,\sigma)= -\sum_{i=1}^n[\delta_{i}\log\sigma-\delta_{i}\epsilon_{i}+e^{\epsilon_{i}}]
\]`
Due to minimizing the negative log-likelihood equivalent to maximum a posteriori probability,
the loss function we use to optimize is $-\iota(\beta,\sigma)$.
The gradient functions for $\beta$ and $\log\sigma$ respectively are:
`\[   
\frac{\partial (-\iota)}{\partial \beta}=\sum_{1=1}^{n}[\delta_{i}-e^{\epsilon_{i}}]\frac{x_{i}}{\sigma}
\]`
`\[ 
\frac{\partial (-\iota)}{\partial (\log\sigma)}=\sum_{i=1}^{n}[\delta_{i}+(\delta_{i}-e^{\epsilon_{i}})\epsilon_{i}]
\]`

The AFT model can be formulated as a convex optimization problem, 
i.e. the task of finding a minimizer of a convex function $-\iota(\beta,\sigma)$ 
that depends coefficients vector $\beta$ and the log of scale parameter $\log\sigma$.
The optimization algorithm underlying the implementation is L-BFGS.
The implementation matches the result from R's survival function 
[survreg](https://stat.ethz.ch/R-manual/R-devel/library/survival/html/survreg.html)

**Example**

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/AFTSurvivalRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaAFTSurvivalRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">
{% include_example python/ml/aft_survival_regression.py %}
</div>

</div>



# Decision trees

[Decision trees](http://en.wikipedia.org/wiki/Decision_tree_learning)
and their ensembles are popular methods for the machine learning tasks of
classification and regression. Decision trees are widely used since they are easy to interpret,
handle categorical features, extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions. Tree ensemble
algorithms such as random forests and boosting are among the top performers for classification and
regression tasks.

The `spark.ml` implementation supports decision trees for binary and multiclass classification and for regression,
using both continuous and categorical features. The implementation partitions data by rows,
allowing distributed training with millions or even billions of instances.

Users can find more information about the decision tree algorithm in the [MLlib Decision Tree guide](mllib-decision-tree.html).
The main differences between this API and the [original MLlib Decision Tree API](mllib-decision-tree.html) are:

* support for ML Pipelines
* separation of Decision Trees for classification vs. regression
* use of DataFrame metadata to distinguish continuous and categorical features


The Pipelines API for Decision Trees offers a bit more functionality than the original API.  In particular, for classification, users can get the predicted probability of each class (a.k.a. class conditional probabilities).

Ensembles of trees (Random Forests and Gradient-Boosted Trees) are described below in the [Tree ensembles section](#tree-ensembles).

## Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

### Input Columns

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

### Output Columns

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


# Tree Ensembles

The DataFrame API supports two major tree ensemble algorithms: [Random Forests](http://en.wikipedia.org/wiki/Random_forest) and [Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting).
Both use [`spark.ml` decision trees](ml-classification-regression.html#decision-trees) as their base models.

Users can find more information about ensemble algorithms in the [MLlib Ensemble guide](mllib-ensembles.html).  
In this section, we demonstrate the DataFrame API for ensembles.

The main differences between this API and the [original MLlib ensembles API](mllib-ensembles.html) are:

* support for DataFrames and ML Pipelines
* separation of classification vs. regression
* use of DataFrame metadata to distinguish continuous and categorical features
* more functionality for random forests: estimates of feature importance, as well as the predicted probability of each class (a.k.a. class conditional probabilities) for classification.

## Random Forests

[Random forests](http://en.wikipedia.org/wiki/Random_forest)
are ensembles of [decision trees](ml-decision-tree.html).
Random forests combine many decision trees in order to reduce the risk of overfitting.
The `spark.ml` implementation supports random forests for binary and multiclass classification and for regression,
using both continuous and categorical features.

For more information on the algorithm itself, please see the [`spark.mllib` documentation on random forests](mllib-ensembles.html).

### Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

#### Input Columns

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

#### Output Columns (Predictions)

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



## Gradient-Boosted Trees (GBTs)

[Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting)
are ensembles of [decision trees](ml-decision-tree.html).
GBTs iteratively train decision trees in order to minimize a loss function.
The `spark.ml` implementation supports GBTs for binary classification and for regression,
using both continuous and categorical features.

For more information on the algorithm itself, please see the [`spark.mllib` documentation on GBTs](mllib-ensembles.html).

### Inputs and Outputs

We list the input and output (prediction) column types here.
All output columns are optional; to exclude an output column, set its corresponding Param to an empty string.

#### Input Columns

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

#### Output Columns (Predictions)

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

