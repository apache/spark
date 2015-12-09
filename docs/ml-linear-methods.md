---
layout: global
title: Linear Methods - ML
displayTitle: <a href="ml-guide.html">ML</a> - Linear Methods
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


In MLlib, we implement popular linear methods such as logistic
regression and linear least squares with $L_1$ or $L_2$ regularization.
Refer to [the linear methods in mllib](mllib-linear-methods.html) for
details.  In `spark.ml`, we also include Pipelines API for [Elastic
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

## Example: Logistic Regression

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
predictions and metrics which are stored as `Dataframe` in
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

## Example: Linear Regression

The interface for working with linear regression models and model
summaries is similar to the logistic regression case. The following
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

# Optimization

The optimization algorithm underlying the implementation is called
[Orthant-Wise Limited-memory
QuasiNewton](http://research-srv.microsoft.com/en-us/um/people/jfgao/paper/icml07scalable.pdf)
(OWL-QN). It is an extension of L-BFGS that can effectively handle L1
regularization and elastic net.

