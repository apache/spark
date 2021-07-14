---
layout: global
title: Classification and regression
displayTitle: Classification and regression
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

This page covers algorithms for Classification and Regression.  It also includes sections
discussing specific classes of algorithms, such as linear methods, trees, and ensembles.

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

# Classification

## Logistic regression

Logistic regression is a popular method to predict a categorical response. It is a special case of [Generalized Linear models](https://en.wikipedia.org/wiki/Generalized_linear_model) that predicts the probability of the outcomes.
In `spark.ml` logistic regression can be used to predict a binary outcome by using binomial logistic regression, or it can be used to predict a multiclass outcome by using multinomial logistic regression. Use the `family`
parameter to select between these two algorithms, or leave it unset and Spark will infer the correct variant.

  > Multinomial logistic regression can be used for binary classification by setting the `family` param to "multinomial". It will produce two sets of coefficients and two intercepts.

  > When fitting LogisticRegressionModel without intercept on dataset with constant nonzero column, Spark MLlib outputs zero coefficients for constant nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.

### Binomial logistic regression

For more background and more details about the implementation of binomial logistic regression, refer to the documentation of [logistic regression in `spark.mllib`](mllib-linear-methods.html#logistic-regression).

**Examples**

The following example shows how to train binomial and multinomial logistic regression
models for binary classification with elastic net regularization. `elasticNetParam` corresponds to
$\alpha$ and `regParam` corresponds to $\lambda$.

<div class="codetabs">

<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/org/apache/spark/ml/classification/LogisticRegression.html).

{% include_example scala/org/apache/spark/examples/ml/LogisticRegressionWithElasticNetExample.scala %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/classification/LogisticRegression.html).

{% include_example java/org/apache/spark/examples/ml/JavaLogisticRegressionWithElasticNetExample.java %}
</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/reference/api/pyspark.ml.classification.LogisticRegression.html).

{% include_example python/ml/logistic_regression_with_elastic_net.py %}
</div>

<div data-lang="r" markdown="1">

More details on parameters can be found in the [R API documentation](api/R/spark.logit.html).

{% include_example binomial r/ml/logit.R %}
</div>

</div>

The `spark.ml` implementation of logistic regression also supports
extracting a summary of the model over the training set. Note that the
predictions and metrics which are stored as `DataFrame` in
`LogisticRegressionSummary` are annotated `@transient` and hence
only available on the driver.

<div class="codetabs">

<div data-lang="scala" markdown="1">

[`LogisticRegressionTrainingSummary`](api/scala/org/apache/spark/ml/classification/LogisticRegressionTrainingSummary.html)
provides a summary for a
[`LogisticRegressionModel`](api/scala/org/apache/spark/ml/classification/LogisticRegressionModel.html).
In the case of binary classification, certain additional metrics are
available, e.g. ROC curve. The binary summary can be accessed via the
`binarySummary` method. See [`BinaryLogisticRegressionTrainingSummary`](api/scala/org/apache/spark/ml/classification/BinaryLogisticRegressionTrainingSummary.html).

Continuing the earlier example:

{% include_example scala/org/apache/spark/examples/ml/LogisticRegressionSummaryExample.scala %}
</div>

<div data-lang="java" markdown="1">
[`LogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/LogisticRegressionTrainingSummary.html)
provides a summary for a
[`LogisticRegressionModel`](api/java/org/apache/spark/ml/classification/LogisticRegressionModel.html).
In the case of binary classification, certain additional metrics are
available, e.g. ROC curve. The binary summary can be accessed via the
`binarySummary` method. See [`BinaryLogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/BinaryLogisticRegressionTrainingSummary.html).

Continuing the earlier example:

{% include_example java/org/apache/spark/examples/ml/JavaLogisticRegressionSummaryExample.java %}
</div>

<div data-lang="python" markdown="1">
[`LogisticRegressionTrainingSummary`](api/python/reference/api/pyspark.ml.classification.LogisticRegressionSummary.html)
provides a summary for a
[`LogisticRegressionModel`](api/python/reference/api/pyspark.ml.classification.LogisticRegressionModel.html).
In the case of binary classification, certain additional metrics are
available, e.g. ROC curve. See [`BinaryLogisticRegressionTrainingSummary`](api/python/reference/api/pyspark.ml.classification.BinaryLogisticRegressionTrainingSummary.html).

Continuing the earlier example:

{% include_example python/ml/logistic_regression_summary_example.py %}
</div>

</div>

### Multinomial logistic regression

Multiclass classification is supported via multinomial logistic (softmax) regression. In multinomial logistic regression,
the algorithm produces $K$ sets of coefficients, or a matrix of dimension $K \times J$ where $K$ is the number of outcome
classes and $J$ is the number of features. If the algorithm is fit with an intercept term then a length $K$ vector of
intercepts is available.

  > Multinomial coefficients are available as `coefficientMatrix` and intercepts are available as `interceptVector`.

  > `coefficients` and `intercept` methods on a logistic regression model trained with multinomial family are not supported. Use `coefficientMatrix` and `interceptVector` instead.

The conditional probabilities of the outcome classes $k \in \{1, 2, ..., K\}$ are modeled using the softmax function.

`\[
   P(Y=k|\mathbf{X}, \boldsymbol{\beta}_k, \beta_{0k}) =  \frac{e^{\boldsymbol{\beta}_k \cdot \mathbf{X}  + \beta_{0k}}}{\sum_{k'=0}^{K-1} e^{\boldsymbol{\beta}_{k'} \cdot \mathbf{X}  + \beta_{0k'}}}
\]`

We minimize the weighted negative log-likelihood, using a multinomial response model, with elastic-net penalty to control for overfitting.

`\[
\min_{\beta, \beta_0} -\left[\sum_{i=1}^L w_i \cdot \log P(Y = y_i|\mathbf{x}_i)\right] + \lambda \left[\frac{1}{2}\left(1 - \alpha\right)||\boldsymbol{\beta}||_2^2 + \alpha ||\boldsymbol{\beta}||_1\right]
\]`

For a detailed derivation please see [here](https://en.wikipedia.org/wiki/Multinomial_logistic_regression#As_a_log-linear_model).

**Examples**

The following example shows how to train a multiclass logistic regression
model with elastic net regularization, as well as extract the multiclass
training summary for evaluating the model.

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% include_example scala/org/apache/spark/examples/ml/MulticlassLogisticRegressionWithElasticNetExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaMulticlassLogisticRegressionWithElasticNetExample.java %}
</div>

<div data-lang="python" markdown="1">
{% include_example python/ml/multiclass_logistic_regression_with_elastic_net.py %}
</div>

<div data-lang="r" markdown="1">

More details on parameters can be found in the [R API documentation](api/R/spark.logit.html).

{% include_example multinomial r/ml/logit.R %}
</div>

</div>


## Decision tree classifier

Decision trees are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on decision trees](#decision-trees).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the Decision Tree algorithm can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/org/apache/spark/ml/classification/DecisionTreeClassifier.html).

{% include_example scala/org/apache/spark/examples/ml/DecisionTreeClassificationExample.scala %}

</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html).

{% include_example java/org/apache/spark/examples/ml/JavaDecisionTreeClassificationExample.java %}

</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/reference/api/pyspark.ml.classification.DecisionTreeClassifier.html).

{% include_example python/ml/decision_tree_classification_example.py %}

</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.decisionTree.html) for more details.

{% include_example classification r/ml/decisionTree.R %}

</div>

</div>

## Random forest classifier

Random forests are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on random forests](#random-forests).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/RandomForestClassifier.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/RandomForestClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/RandomForestClassifier.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaRandomForestClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.RandomForestClassifier.html) for more details.

{% include_example python/ml/random_forest_classifier_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.randomForest.html) for more details.

{% include_example classification r/ml/randomForest.R %}
</div>

</div>

## Gradient-boosted tree classifier

Gradient-boosted trees (GBTs) are a popular classification and regression method using ensembles of decision trees.
More information about the `spark.ml` implementation can be found further in the [section on GBTs](#gradient-boosted-trees-gbts).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use two feature transformers to prepare the data; these help index categories for the label and categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/GBTClassifier.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/GradientBoostedTreeClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/GBTClassifier.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaGradientBoostedTreeClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.GBTClassifier.html) for more details.

{% include_example python/ml/gradient_boosted_tree_classifier_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.gbt.html) for more details.

{% include_example classification r/ml/gbt.R %}
</div>

</div>

## Multilayer perceptron classifier

Multilayer perceptron classifier (MLPC) is a classifier based on the [feedforward artificial neural network](https://en.wikipedia.org/wiki/Feedforward_neural_network).
MLPC consists of multiple layers of nodes.
Each layer is fully connected to the next layer in the network. Nodes in the input layer represent the input data. All other nodes map inputs to outputs
by a linear combination of the inputs with the node's weights `$\wv$` and bias `$\bv$` and applying an activation function.
This can be written in matrix form for MLPC with `$K+1$` layers as follows:
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

MLPC employs backpropagation for learning the model. We use the logistic loss function for optimization and L-BFGS as an optimization routine.

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/MultilayerPerceptronClassifier.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/MultilayerPerceptronClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/MultilayerPerceptronClassifier.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaMultilayerPerceptronClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.MultilayerPerceptronClassifier.html) for more details.

{% include_example python/ml/multilayer_perceptron_classification.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.mlp.html) for more details.

{% include_example r/ml/mlp.R %}
</div>

</div>

## Linear Support Vector Machine

A [support vector machine](https://en.wikipedia.org/wiki/Support_vector_machine) constructs a hyperplane
or set of hyperplanes in a high- or infinite-dimensional space, which can be used for classification,
regression, or other tasks. Intuitively, a good separation is achieved by the hyperplane that has
the largest distance to the nearest training-data points of any class (so-called functional margin),
since in general the larger the margin the lower the generalization error of the classifier. LinearSVC
in Spark ML supports binary classification with linear SVM. Internally, it optimizes the
[Hinge Loss](https://en.wikipedia.org/wiki/Hinge_loss) using OWLQN optimizer.


**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/LinearSVC.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/LinearSVCExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/LinearSVC.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaLinearSVCExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.LinearSVC.html) for more details.

{% include_example python/ml/linearsvc.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.svmLinear.html) for more details.

{% include_example r/ml/svmLinear.R %}
</div>

</div>

## One-vs-Rest classifier (a.k.a. One-vs-All)

[OneVsRest](http://en.wikipedia.org/wiki/Multiclass_classification#One-vs.-rest) is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently.  It is also known as "One-vs-All."

`OneVsRest` is implemented as an `Estimator`. For the base classifier, it takes instances of `Classifier` and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.

Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.

**Examples**

The example below demonstrates how to load the
[Iris dataset](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale), parse it as a DataFrame and perform multiclass classification using `OneVsRest`. The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/OneVsRest.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/OneVsRestExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/OneVsRest.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaOneVsRestExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.OneVsRest.html) for more details.

{% include_example python/ml/one_vs_rest_example.py %}
</div>
</div>

## Naive Bayes

[Naive Bayes classifiers](http://en.wikipedia.org/wiki/Naive_Bayes_classifier) are a family of simple
probabilistic, multiclass classifiers based on applying Bayes' theorem with strong (naive) independence
assumptions between every pair of features.

Naive Bayes can be trained very efficiently. With a single pass over the training data,
it computes the conditional probability distribution of each feature given each label.
For prediction, it applies Bayes' theorem to compute the conditional probability distribution
of each label given an observation.

MLlib supports [Multinomial naive Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier#Multinomial_naive_Bayes),
[Complement naive Bayes](https://people.csail.mit.edu/jrennie/papers/icml03-nb.pdf),
[Bernoulli naive Bayes](http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html)
and [Gaussian naive Bayes](https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Gaussian_naive_Bayes).

*Input data*:
These Multinomial, Complement and Bernoulli models are typically used for [document classification](http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html).
Within that context, each observation is a document and each feature represents a term.
A feature's value is the frequency of the term (in Multinomial or Complement Naive Bayes) or
a zero or one indicating whether the term was found in the document (in Bernoulli Naive Bayes).
Feature values for Multinomial and Bernoulli models must be *non-negative*. The model type is selected with an optional parameter
"multinomial", "complement", "bernoulli" or "gaussian", with "multinomial" as the default.
For document classification, the input feature vectors should usually be sparse vectors.
Since the training data is only used once, it is not necessary to cache it.

[Additive smoothing](http://en.wikipedia.org/wiki/Lidstone_smoothing) can be used by
setting the parameter $\lambda$ (default to $1.0$).

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/NaiveBayes.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/NaiveBayesExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/NaiveBayes.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaNaiveBayesExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.NaiveBayes.html) for more details.

{% include_example python/ml/naive_bayes_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.naiveBayes.html) for more details.

{% include_example r/ml/naiveBayes.R %}
</div>

</div>


## Factorization machines classifier

For more background and more details about the implementation of factorization machines,
refer to the [Factorization Machines section](ml-classification-regression.html#factorization-machines).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets,
train on the first dataset, and then evaluate on the held-out test set.
We scale features to be between 0 and 1 to prevent the exploding gradient problem.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/classification/FMClassifier.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/FMClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/classification/FMClassifier.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaFMClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.classification.FMClassifier.html) for more details.

{% include_example python/ml/fm_classifier_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.fmClassifier.html) for more details.

Note: At the moment SparkR doesn't support feature scaling.

{% include_example r/ml/fmClassifier.R %}
</div>

</div>


# Regression

## Linear regression

The interface for working with linear regression models and model
summaries is similar to the logistic regression case.

  > When fitting LinearRegressionModel without intercept on dataset with constant nonzero column by "l-bfgs" solver, Spark MLlib outputs zero coefficients for constant nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.

**Examples**

The following
example demonstrates training an elastic net regularized linear
regression model and extracting model summary statistics.

<div class="codetabs">

<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/org/apache/spark/ml/regression/LinearRegression.html).

{% include_example scala/org/apache/spark/examples/ml/LinearRegressionWithElasticNetExample.scala %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/regression/LinearRegression.html).

{% include_example java/org/apache/spark/examples/ml/JavaLinearRegressionWithElasticNetExample.java %}
</div>

<div data-lang="python" markdown="1">
<!--- TODO: Add python model summaries once implemented -->

More details on parameters can be found in the [Python API documentation](api/python/reference/api/pyspark.ml.regression.LinearRegression.html#pyspark.ml.regression.LinearRegression).

{% include_example python/ml/linear_regression_with_elastic_net.py %}
</div>

<div data-lang="r" markdown="1">

More details on parameters can be found in the [R API documentation](api/R/spark.lm.html).

{% include_example r/ml/lm_with_elastic_net.R %}
</div>

</div>

## Generalized linear regression

Contrasted with linear regression where the output is assumed to follow a Gaussian
distribution, [generalized linear models](https://en.wikipedia.org/wiki/Generalized_linear_model) (GLMs) are specifications of linear models where the response variable $Y_i$ follows some
distribution from the [exponential family of distributions](https://en.wikipedia.org/wiki/Exponential_family).
Spark's `GeneralizedLinearRegression` interface
allows for flexible specification of GLMs which can be used for various types of
prediction problems including linear regression, Poisson regression, logistic regression, and others.
Currently in `spark.ml`, only a subset of the exponential family distributions are supported and they are listed
[below](#available-families).

**NOTE**: Spark currently only supports up to 4096 features through its `GeneralizedLinearRegression`
interface, and will throw an exception if this constraint is exceeded. See the [advanced section](ml-advanced) for more details.
 Still, for linear and logistic regression, models with an increased number of features can be trained
 using the `LinearRegression` and `LogisticRegression` estimators.

GLMs require exponential family distributions that can be written in their "canonical" or "natural" form, aka
[natural exponential family distributions](https://en.wikipedia.org/wiki/Natural_exponential_family). The form of a natural exponential family distribution is given as:

$$
f_Y(y|\theta, \tau) = h(y, \tau)\exp{\left( \frac{\theta \cdot y - A(\theta)}{d(\tau)} \right)}
$$

where $\theta$ is the parameter of interest and $\tau$ is a dispersion parameter. In a GLM the response variable $Y_i$ is assumed to be drawn from a natural exponential family distribution:

$$
Y_i \sim f\left(\cdot|\theta_i, \tau \right)
$$

where the parameter of interest $\theta_i$ is related to the expected value of the response variable $\mu_i$ by

$$
\mu_i = A'(\theta_i)
$$

Here, $A'(\theta_i)$ is defined by the form of the distribution selected. GLMs also allow specification
of a link function, which defines the relationship between the expected value of the response variable $\mu_i$
and the so called _linear predictor_ $\eta_i$:

$$
g(\mu_i) = \eta_i = \vec{x_i}^T \cdot \vec{\beta}
$$

Often, the link function is chosen such that $A' = g^{-1}$, which yields a simplified relationship
between the parameter of interest $\theta$ and the linear predictor $\eta$. In this case, the link
function $g(\mu)$ is said to be the "canonical" link function.

$$
\theta_i = A'^{-1}(\mu_i) = g(g^{-1}(\eta_i)) = \eta_i
$$

A GLM finds the regression coefficients $\vec{\beta}$ which maximize the likelihood function.

$$
\max_{\vec{\beta}} \mathcal{L}(\vec{\theta}|\vec{y},X) =
\prod_{i=1}^{N} h(y_i, \tau) \exp{\left(\frac{y_i\theta_i - A(\theta_i)}{d(\tau)}\right)}
$$

where the parameter of interest $\theta_i$ is related to the regression coefficients $\vec{\beta}$
by

$$
\theta_i = A'^{-1}(g^{-1}(\vec{x_i} \cdot \vec{\beta}))
$$

Spark's generalized linear regression interface also provides summary statistics for diagnosing the
fit of GLM models, including residuals, p-values, deviances, the Akaike information criterion, and
others.

[See here](http://data.princeton.edu/wws509/notes/) for a more comprehensive review of GLMs and their applications.

###  Available families

<table class="table">
  <thead>
    <tr>
      <th>Family</th>
      <th>Response Type</th>
      <th>Supported Links</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Gaussian</td>
      <td>Continuous</td>
      <td>Identity*, Log, Inverse</td>
    </tr>
    <tr>
      <td>Binomial</td>
      <td>Binary</td>
      <td>Logit*, Probit, CLogLog</td>
    </tr>
    <tr>
      <td>Poisson</td>
      <td>Count</td>
      <td>Log*, Identity, Sqrt</td>
    </tr>
    <tr>
      <td>Gamma</td>
      <td>Continuous</td>
      <td>Inverse*, Identity, Log</td>
    </tr>
    <tr>
      <td>Tweedie</td>
      <td>Zero-inflated continuous</td>
      <td>Power link function</td>
    </tr>
    <tfoot><tr><td colspan="4">* Canonical Link</td></tr></tfoot>
  </tbody>
</table>

**Examples**

The following example demonstrates training a GLM with a Gaussian response and identity link
function and extracting model summary statistics.

<div class="codetabs">

<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/regression/GeneralizedLinearRegression.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/GeneralizedLinearRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/GeneralizedLinearRegression.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaGeneralizedLinearRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.regression.GeneralizedLinearRegression.html#pyspark.ml.regression.GeneralizedLinearRegression) for more details.

{% include_example python/ml/generalized_linear_regression_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.glm.html) for more details.

{% include_example r/ml/glm.R %}
</div>

</div>


## Decision tree regression

Decision trees are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on decision trees](#decision-trees).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use a feature transformer to index categorical features, adding metadata to the `DataFrame` which the Decision Tree algorithm can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

More details on parameters can be found in the [Scala API documentation](api/scala/org/apache/spark/ml/regression/DecisionTreeRegressor.html).

{% include_example scala/org/apache/spark/examples/ml/DecisionTreeRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">

More details on parameters can be found in the [Java API documentation](api/java/org/apache/spark/ml/regression/DecisionTreeRegressor.html).

{% include_example java/org/apache/spark/examples/ml/JavaDecisionTreeRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">

More details on parameters can be found in the [Python API documentation](api/python/reference/api/pyspark.ml.regression.DecisionTreeRegressor.html#pyspark.ml.regression.DecisionTreeRegressor).

{% include_example python/ml/decision_tree_regression_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.decisionTree.html) for more details.

{% include_example regression r/ml/decisionTree.R %}
</div>

</div>


## Random forest regression

Random forests are a popular family of classification and regression methods.
More information about the `spark.ml` implementation can be found further in the [section on random forests](#random-forests).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets, train on the first dataset, and then evaluate on the held-out test set.
We use a feature transformer to index categorical features, adding metadata to the `DataFrame` which the tree-based algorithms can recognize.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/regression/RandomForestRegressor.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/RandomForestRegressorExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/RandomForestRegressor.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaRandomForestRegressorExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.regression.RandomForestRegressor.html#pyspark.ml.regression.RandomForestRegressor) for more details.

{% include_example python/ml/random_forest_regressor_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.randomForest.html) for more details.

{% include_example regression r/ml/randomForest.R %}
</div>

</div>

## Gradient-boosted tree regression

Gradient-boosted trees (GBTs) are a popular regression method using ensembles of decision trees.
More information about the `spark.ml` implementation can be found further in the [section on GBTs](#gradient-boosted-trees-gbts).

**Examples**

Note: For this example dataset, `GBTRegressor` actually only needs 1 iteration, but that will not
be true in general.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/regression/GBTRegressor.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/GradientBoostedTreeRegressorExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/GBTRegressor.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaGradientBoostedTreeRegressorExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.regression.GBTRegressor.html#pyspark.ml.regression.GBTRegressor) for more details.

{% include_example python/ml/gradient_boosted_tree_regressor_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.gbt.html) for more details.

{% include_example regression r/ml/gbt.R %}
</div>

</div>


## Survival regression


In `spark.ml`, we implement the [Accelerated failure time (AFT)](https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
model which is a parametric survival regression model for censored data.
It describes a model for the log of survival time, so it's often called a
log-linear model for survival analysis. Different from a
[Proportional hazards](https://en.wikipedia.org/wiki/Proportional_hazards_model) model
designed for the same purpose, the AFT model is easier to parallelize
because each instance contributes to the objective function independently.

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
and $f_{0}(\epsilon_{i})$ is the corresponding density function.

The most commonly used AFT model is based on the Weibull distribution of the survival time.
The Weibull distribution for lifetime corresponds to the extreme value distribution for the
log of the lifetime, and the $S_{0}(\epsilon)$ function is:
`\[   
S_{0}(\epsilon_{i})=\exp(-e^{\epsilon_{i}})
\]`
the $f_{0}(\epsilon_{i})$ function is:
`\[
f_{0}(\epsilon_{i})=e^{\epsilon_{i}}\exp(-e^{\epsilon_{i}})
\]`
The log-likelihood function for AFT model with a Weibull distribution of lifetime is:
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
that depends on the coefficients vector $\beta$ and the log of scale parameter $\log\sigma$.
The optimization algorithm underlying the implementation is L-BFGS.
The implementation matches the result from R's survival function
[survreg](https://stat.ethz.ch/R-manual/R-devel/library/survival/html/survreg.html)

  > When fitting AFTSurvivalRegressionModel without intercept on dataset with constant nonzero column, Spark MLlib outputs zero coefficients for constant nonzero columns. This behavior is different from R survival::survreg.

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/regression/AFTSurvivalRegression.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/AFTSurvivalRegressionExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/AFTSurvivalRegression.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaAFTSurvivalRegressionExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.regression.AFTSurvivalRegression.html#pyspark.ml.regression.AFTSurvivalRegression) for more details.

{% include_example python/ml/aft_survival_regression.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API docs](api/R/spark.survreg.html) for more details.

{% include_example r/ml/survreg.R %}
</div>

</div>


## Isotonic regression
[Isotonic regression](http://en.wikipedia.org/wiki/Isotonic_regression)
belongs to the family of regression algorithms. Formally isotonic regression is a problem where
given a finite set of real numbers `$Y = {y_1, y_2, ..., y_n}$` representing observed responses
and `$X = {x_1, x_2, ..., x_n}$` the unknown response values to be fitted
finding a function that minimizes

`\begin{equation}
  f(x) = \sum_{i=1}^n w_i (y_i - x_i)^2
\end{equation}`

with respect to complete order subject to
`$x_1\le x_2\le ...\le x_n$` where `$w_i$` are positive weights.
The resulting function is called isotonic regression and it is unique.
It can be viewed as least squares problem under order restriction.
Essentially isotonic regression is a
[monotonic function](http://en.wikipedia.org/wiki/Monotonic_function)
best fitting the original data points.

We implement a
[pool adjacent violators algorithm](https://doi.org/10.1198/TECH.2010.10111)
which uses an approach to
[parallelizing isotonic regression](https://doi.org/10.1007/978-3-642-99789-1_10).
The training input is a DataFrame which contains three columns
label, features and weight. Additionally, IsotonicRegression algorithm has one
optional parameter called $isotonic$ defaulting to true.
This argument specifies if the isotonic regression is
isotonic (monotonically increasing) or antitonic (monotonically decreasing).

Training returns an IsotonicRegressionModel that can be used to predict
labels for both known and unknown features. The result of isotonic regression
is treated as piecewise linear function. The rules for prediction therefore are:

* If the prediction input exactly matches a training feature
  then associated prediction is returned. In case there are multiple predictions with the same
  feature then one of them is returned. Which one is undefined
  (same as java.util.Arrays.binarySearch).
* If the prediction input is lower or higher than all training features
  then prediction with lowest or highest feature is returned respectively.
  In case there are multiple predictions with the same feature
  then the lowest or highest is returned respectively.
* If the prediction input falls between two training features then prediction is treated
  as piecewise linear function and interpolated value is calculated from the
  predictions of the two closest features. In case there are multiple values
  with the same feature then the same rules as in previous point are used.

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [`IsotonicRegression` Scala docs](api/scala/org/apache/spark/ml/regression/IsotonicRegression.html) for details on the API.

{% include_example scala/org/apache/spark/examples/ml/IsotonicRegressionExample.scala %}
</div>
<div data-lang="java" markdown="1">

Refer to the [`IsotonicRegression` Java docs](api/java/org/apache/spark/ml/regression/IsotonicRegression.html) for details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaIsotonicRegressionExample.java %}
</div>
<div data-lang="python" markdown="1">

Refer to the [`IsotonicRegression` Python docs](api/python/reference/api/pyspark.ml.regression.IsotonicRegression.html#pyspark.ml.regression.IsotonicRegression) for more details on the API.

{% include_example python/ml/isotonic_regression_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [`IsotonicRegression` R API docs](api/R/spark.isoreg.html) for more details on the API.

{% include_example r/ml/isoreg.R %}
</div>

</div>


## Factorization machines regressor

For more background and more details about the implementation of factorization machines,
refer to the [Factorization Machines section](ml-classification-regression.html#factorization-machines).

**Examples**

The following examples load a dataset in LibSVM format, split it into training and test sets,
train on the first dataset, and then evaluate on the held-out test set.
We scale features to be between 0 and 1 to prevent the exploding gradient problem.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Scala API docs](api/scala/org/apache/spark/ml/regression/FMRegressor.html) for more details.

{% include_example scala/org/apache/spark/examples/ml/FMRegressorExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Java API docs](api/java/org/apache/spark/ml/regression/FMRegressor.html) for more details.

{% include_example java/org/apache/spark/examples/ml/JavaFMRegressorExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Python API docs](api/python/reference/api/pyspark.ml.regression.FMRegressor.html#pyspark.ml.regression.FMRegressor) for more details.

{% include_example python/ml/fm_regressor_example.py %}
</div>

<div data-lang="r" markdown="1">

Refer to the [R API documentation](api/R/spark.fmRegressor.html) for more details.

Note: At the moment SparkR doesn't support feature scaling.

{% include_example r/ml/fmRegressor.R %}
</div>

</div>


# Linear methods

We implement popular linear methods such as logistic
regression and linear least squares with $L_1$ or $L_2$ regularization.
Refer to [the linear methods guide for the RDD-based API](mllib-linear-methods.html) for
details about implementation and tuning; this information is still relevant.

We also include a DataFrame API for [Elastic
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

# Factorization Machines

[Factorization Machines](https://www.csie.ntu.edu.tw/~b97053/paper/Rendle2010FM.pdf) are able to estimate interactions
between features even in problems with huge sparsity (like advertising and recommendation system).
The `spark.ml` implementation supports factorization machines for binary classification and for regression.

Factorization machines formula is:

$$
\hat{y} = w_0 + \sum\limits^n_{i-1} w_i x_i +
  \sum\limits^n_{i=1} \sum\limits^n_{j=i+1} \langle v_i, v_j \rangle x_i x_j
$$

The first two terms denote intercept and linear term (same as in linear regression),
and the last term denotes pairwise interactions term. $$v_i$$ describes the i-th variable
with k factors.

FM can be used for regression and optimization criterion is mean square error. FM also can be used for
binary classification through sigmoid function. The optimization criterion is logistic loss.

The pairwise interactions can be reformulated:

$$
\sum\limits^n_{i=1} \sum\limits^n_{j=i+1} \langle v_i, v_j \rangle x_i x_j
  = \frac{1}{2}\sum\limits^k_{f=1}
    \left(\left( \sum\limits^n_{i=1}v_{i,f}x_i \right)^2 -
    \sum\limits^n_{i=1}v_{i,f}^2x_i^2 \right)
$$

This equation has only linear complexity in both k and n - i.e. its computation is in $$O(kn)$$.

In general, in order to prevent the exploding gradient problem, it is best to scale continuous features to be between 0 and 1,
or bin the continuous features and one-hot encode them.

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


The Pipelines API for Decision Trees offers a bit more functionality than the original API.  
In particular, for classification, users can get the predicted probability of each class (a.k.a. class conditional probabilities);
for regression, users can get the biased sample variance of prediction.

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
    <tr>
      <td>varianceCol</td>
      <td>Double</td>
      <td></td>
      <td>The biased sample variance of prediction</td>
      <td>Regression only</td>
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
are ensembles of [decision trees](ml-classification-regression.html#decision-trees).
Random forests combine many decision trees in order to reduce the risk of overfitting.
The `spark.ml` implementation supports random forests for binary and multiclass classification and for regression,
using both continuous and categorical features.

For more information on the algorithm itself, please see the [`spark.mllib` documentation on random forests](mllib-ensembles.html#random-forests).

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
are ensembles of [decision trees](ml-classification-regression.html#decision-trees).
GBTs iteratively train decision trees in order to minimize a loss function.
The `spark.ml` implementation supports GBTs for binary classification and for regression,
using both continuous and categorical features.

For more information on the algorithm itself, please see the [`spark.mllib` documentation on GBTs](mllib-ensembles.html#gradient-boosted-trees-gbts).

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
