---
layout: global
title: MLlib - Classification and Regression
---

* Table of contents
{:toc}


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


# Supervised Machine Learning
Supervised machine learning is the setting where we are given a set of training data examples
`$\{\x_i\}$`, each example `$\x_i$` coming with a corresponding label `$y_i$`.
Given the training data `$\{(\x_i,y_i)\}$`, we want to learn a function to predict these labels.
The two most well known classes of methods are
[classification](http://en.wikipedia.org/wiki/Statistical_classification), and
[regression](http://en.wikipedia.org/wiki/Regression_analysis).
In classification, the label is a category (e.g. whether or not emails are spam), whereas in
regression, the label is real value, and we want our prediction to be as close to the true value
as possible.

Supervised Learning involves executing a learning *Algorithm* on a set of *labeled* training
examples. The algorithm returns a trained *Model* (such as for example a linear function) that
can predict the label for new data examples for which the label is unknown.


## Mathematical Formulation
Many standard *machine learning* methods can be formulated as a convex optimization problem, i.e.
the task of finding a minimizer of a convex function `$f$` that depends on a variable vector
`$\wv$` (called `weights` in the code), which has `$d$` entries. 
Formally, we can write this as the optimization problem `$\min_{\wv \in\R^d} \; f(\wv)$`, where
the objective function is of the form
`\begin{equation}
    f(\wv) := 
    \lambda\, R(\wv) +
    \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i) 
    \label{eq:regPrimal}
    \ .
\end{equation}`
Here the vectors `$\x_i\in\R^d$` are the training data examples, for `$1\le i\le n$`, and
`$y_i\in\R$` are their corresponding labels, which we want to predict. 

The objective function `$f$` has two parts:
The *loss-function* measures the error of the model on the training data. The loss-function
`$L(\wv;.)$` must be a convex function in `$\wv$`.
The purpose of the [regularizer](http://en.wikipedia.org/wiki/Regularization_(mathematics)) is to
encourage simple models, by punishing the complexity of the model `$\wv$`, in order to e.g. avoid
over-fitting.
Usually, the regularizer `$R(.)$` is chosen as either the standard (Euclidean) L2-norm, `$R(\wv)
:= \frac{1}{2}\|\wv\|^2$`, or the L1-norm, `$R(\wv) := \|\wv\|_1$`, see
[below](#using-different-regularizers) for more details.

The fixed regularization parameter `$\lambda\ge0$` (`regParam` in the code) defines the trade-off
between the two goals of small loss and small model complexity.


## Binary Classification

**Input:** Datapoints `$\x_i\in\R^{d}$`, labels `$y_i\in\{+1,-1\}$`, for `$1\le i\le n$`.

**Distributed Datasets.**
For all currently implemented optimization methods for classification, the data must be
distributed between the worker machines *by examples*. Every machine holds a consecutive block of
the `$n$` example/label pairs `$(\x_i,y_i)$`. 
In other words, the input distributed dataset
([RDD](scala-programming-guide.html#resilient-distributed-datasets-rdds)) must be the set of
vectors `$\x_i\in\R^d$`.

### Support Vector Machine
The linear [Support Vector Machine (SVM)](http://en.wikipedia.org/wiki/Support_vector_machine)
has become a standard choice for classification tasks.
Here the loss function in formulation `$\eqref{eq:regPrimal}$` is given by the hinge-loss 
`\[
L(\wv;\x_i,y_i) := \max \{0, 1-y_i \wv^T \x_i \} \ .
\]`

By default, SVMs are trained with an L2 regularization, which gives rise to the large-margin
interpretation if these classifiers. We also support alternative L1 regularization. In this case,
the primal optimization problem becomes an [LP](http://en.wikipedia.org/wiki/Linear_programming).

### Logistic Regression
Despite its name, [Logistic Regression](http://en.wikipedia.org/wiki/Logistic_regression) is a
binary classification method, again when the labels are given by binary values
`$y_i\in\{+1,-1\}$`. The logistic loss function in formulation `$\eqref{eq:regPrimal}$` is
defined as
`\[
L(\wv;\x_i,y_i) :=  \log(1+\exp( -y_i \wv^T \x_i)) \ .
\]`


## Linear Regression (Least Squares, Lasso and Ridge Regression)

**Input:** Data matrix `$A\in\R^{n\times d}$`, right hand side vector `$\y\in\R^n$`.

**Distributed Datasets.**
For all currently implemented optimization methods for regression, the data matrix
`$A\in\R^{n\times d}$` must be distributed between the worker machines *by rows* of `$A$`. In
other words, the input distributed dataset
([RDD](scala-programming-guide.html#resilient-distributed-datasets-rdds)) must be the set of the
`$n$` rows `$A_{i:}$` of `$A$`.

Least Squares Regression refers to the setting where we try to fit a vector `$\y\in\R^n$` by
linear combination of our observed data `$A\in\R^{n\times d}$`, which is given as a matrix.

It comes in 3 flavors:

### Least Squares
Plain old [least squares](http://en.wikipedia.org/wiki/Least_squares) linear regression is the
problem of minimizing 
  `\[ f_{\text{LS}}(\wv) := \frac1n \|A\wv-\y\|_2^2 \ . \]`

### Lasso
The popular [Lasso](http://en.wikipedia.org/wiki/Lasso_(statistics)#Lasso_method) (alternatively
also known as  `$L_1$`-regularized least squares regression) is given by
  `\[ f_{\text{Lasso}}(\wv) := \frac1n \|A\wv-\y\|_2^2  + \lambda \|\wv\|_1 \ . \]`

### Ridge Regression
[Ridge regression](http://en.wikipedia.org/wiki/Ridge_regression) uses the same loss function but
with a L2 regularizer term:
  `\[ f_{\text{Ridge}}(\wv) := \frac1n \|A\wv-\y\|_2^2  + \frac{\lambda}{2}\|\wv\|^2 \ . \]`

**Loss Function.**
For all 3, the loss function (i.e. the measure of model fit) is given by the squared deviations
from the right hand side `$\y$`.
`\[
\frac1n \|A\wv-\y\|_2^2
= \frac1n \sum_{i=1}^n (A_{i:} \wv - y_i )^2
= \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i)
\]`
This is also known as the [mean squared error](http://en.wikipedia.org/wiki/Mean_squared_error).
In our generic problem formulation `$\eqref{eq:regPrimal}$`, this means the loss function is
`$L(\wv;\x_i,y_i) := (A_{i:} \wv - y_i )^2$`, each depending only on a single row `$A_{i:}$` of
the data matrix `$A$`.


## Using Different Regularizers

As we have mentioned above, the purpose of *regularizer* in `$\eqref{eq:regPrimal}$` is to
encourage simple models, by punishing the complexity of the model `$\wv$`, in order to e.g. avoid
over-fitting.
All machine learning methods for classification and regression that we have mentioned above are
of interest for different types of regularization, the 3 most common ones being

* **L2-Regularization.**
`$R(\wv) := \frac{1}{2}\|\wv\|^2$`.
This regularizer is most commonly used for SVMs, logistic regression and ridge regression.

* **L1-Regularization.**
`$R(\wv) := \|\wv\|_1$`. The L1 norm `$\|\wv\|_1$` is the sum of the absolut values of the
entries of a vector `$\wv$`. 
This regularizer is most commonly used for sparse methods, and feature selection, such as the
Lasso.

* **Non-Regularized.**
`$R(\wv):=0$`.
Of course we can also train the models without any regularization, or equivalently by setting the
regularization parameter `$\lambda:=0$`.

The optimization problems of the form `$\eqref{eq:regPrimal}$` with convex regularizers such as
the 3 mentioned here can be conveniently optimized with gradient descent type methods (such as
SGD) which is implemented in `MLlib` currently, and explained in the next section.


# Optimization Methods Working on the Primal Formulation

**Stochastic subGradient Descent (SGD).**
For optimization objectives `$f$` written as a sum, *stochastic subgradient descent (SGD)* can be
an efficient choice of optimization method, as we describe in the <a
href="mllib-optimization.html">optimization section</a> in more detail. 
Because all methods considered here fit into the optimization formulation
`$\eqref{eq:regPrimal}$`, this is especially natural, because the loss is written as an average
of the individual losses coming from each datapoint.

Picking one datapoint `$i\in[1..n]$` uniformly at random, we obtain a stochastic subgradient of
`$\eqref{eq:regPrimal}$`, with respect to `$\wv$` as follows:
`\[
f'_{\wv,i} := L'_{\wv,i} + \lambda\, R'_\wv \ ,
\]`
where `$L'_{\wv,i} \in \R^d$` is a subgradient of the part of the loss function determined by the
`$i$`-th datapoint, that is `$L'_{\wv,i} \in \frac{\partial}{\partial \wv}  L(\wv;\x_i,y_i)$`.
Furthermore, `$R'_\wv$` is a subgradient of the regularizer `$R(\wv)$`, i.e. `$R'_\wv \in
\frac{\partial}{\partial \wv} R(\wv)$`. The term `$R'_\wv$` does not depend on which random
datapoint is picked.



**Gradients.** 
The following table summarizes the gradients (or subgradients) of all loss functions and
regularizers that we currently support:

<table class="table">
  <thead>
    <tr><th></th><th>Function</th><th>Stochastic (Sub)Gradient</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>SVM Hinge Loss</td><td>$L(\wv;\x_i,y_i) := \max \{0, 1-y_i \wv^T \x_i \}$</td>
      <td>$L'_{\wv,i} = \begin{cases}-y_i \x_i & \text{if $y_i \wv^T \x_i <1$}, \\ 0 &
\text{otherwise}.\end{cases}$</td>
    </tr>
    <tr>
      <td>Logistic Loss</td><td>$L(\wv;\x_i,y_i) :=  \log(1+\exp( -y_i \wv^T \x_i))$</td>
      <td>$L'_{\wv,i} = -y_i \x_i  \left(1-\frac1{1+\exp(-y_i \wv^T \x_i)} \right)$</td>
    </tr>
    <tr>
      <td>Least Squares Loss</td><td>$L(\wv;\x_i,y_i) := (A_{i:} \wv - y_i)^2$</td>
      <td>$L'_{\wv,i} = 2 A_{i:}^T (A_{i:} \wv - y_i)$</td>
    </tr>
    <tr>
      <td>Non-Regularized</td><td>$R(\wv) := 0$</td><td>$R'_\wv = \0$</td>
    </tr>
    <tr>
      <td>L2 Regularizer</td><td>$R(\wv) := \frac{1}{2}\|\wv\|^2$</td><td>$R'_\wv = \wv$</td>
    </tr>
    <tr>
      <td>L1 Regularizer</td><td>$R(\wv) := \|\wv\|_1$</td><td>$R'_\wv = \mathop{sign}(\wv)$</td>
    </tr>
  </tbody>
</table>

Here `$\mathop{sign}(\wv)$` is the vector consisting of the signs (`$\pm1$`) of all the entries
of `$\wv$`.
Also, note that `$A_{i:} \in \R^d$` is a row-vector, but the gradient is a column vector.



## Implementation in MLlib

For both classification and regression, `MLlib` implements a simple distributed version of
stochastic subgradient descent (SGD), building on the underlying gradient descent primitive (as
described in the
<a href="mllib-optimization.html">optimization section</a>).
All provided algorithms take as input a regularization parameter (`regParam`) along with various
parameters associated with stochastic gradient
descent (`stepSize`, `numIterations`, `miniBatchFraction`).
For each of them, we support all 3 possible regularizations (none, L1 or L2).

Available algorithms for binary classification:

* [SVMWithSGD](api/mllib/index.html#org.apache.spark.mllib.classification.SVMWithSGD)
* [LogisticRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD)

Available algorithms for linear regression: 

* [LinearRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.regression.LinearRegressionWithSGD)
* [RidgeRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.regression.RidgeRegressionWithSGD)
* [LassoWithSGD](api/mllib/index.html#org.apache.spark.mllib.regression.LassoWithSGD)

Behind the scenes, all above methods use the SGD implementation from the
gradient descent primitive in MLlib, see the 
<a href="mllib-optimization.html">optimization</a> part:

* [GradientDescent](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent)





# Usage in Scala

Following code snippets can be executed in `spark-shell`.

## Binary Classification

The following code snippet illustrates how to load a sample dataset, execute a
training algorithm on this training data using a static method in the algorithm
object, and make predictions with the resulting model to compute the training
error.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

// Load and parse the data file
val data = sc.textFile("mllib/data/sample_svm_data.txt")
val parsedData = data.map { line =>
  val parts = line.split(' ')
  LabeledPoint(parts(0).toDouble, parts.tail.map(x => x.toDouble).toArray)
}

// Run training algorithm to build the model
val numIterations = 20
val model = SVMWithSGD.train(parsedData, numIterations)

// Evaluate model on training examples and compute training error
val labelAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
println("Training Error = " + trainErr)
{% endhighlight %}


The `SVMWithSGD.train()` method by default performs L2 regularization with the
regularization parameter set to 1.0. If we want to configure this algorithm, we
can customize `SVMWithSGD` further by creating a new object directly and
calling setter methods. All other MLlib algorithms support customization in
this way as well. For example, the following code produces an L1 regularized
variant of SVMs with regularization parameter set to 0.1, and runs the training
algorithm for 200 iterations.

{% highlight scala %}
import org.apache.spark.mllib.optimization.L1Updater

val svmAlg = new SVMWithSGD()
svmAlg.optimizer.setNumIterations(200)
  .setRegParam(0.1)
  .setUpdater(new L1Updater)
val modelL1 = svmAlg.run(parsedData)
{% endhighlight %}

## Linear Regression

The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint.
The example then uses LinearRegressionWithSGD to build a simple linear model to predict label 
values. We compute the Mean Squared Error at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit)

{% highlight scala %}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

// Load and parse the data
val data = sc.textFile("mllib/data/ridge-data/lpsa.data")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, parts(1).split(' ').map(x => x.toDouble).toArray)
}

// Building the model
val numIterations = 20
val model = LinearRegressionWithSGD.train(parsedData, numIterations)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.reduce(_ + _)/valuesAndPreds.count
println("training Mean Squared Error = " + MSE)
{% endhighlight %}


Similarly you can use RidgeRegressionWithSGD and LassoWithSGD and compare training
[Mean Squared Errors](http://en.wikipedia.org/wiki/Mean_squared_error).


# Usage in Java

All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object.

# Usage in Python

Following examples can be tested in the PySpark shell.

## Binary Classification
The following example shows how to load a sample dataset, build Logistic Regression model,
and make predictions with the resulting model to compute the training error.

{% highlight python %}
from pyspark.mllib.classification import LogisticRegressionWithSGD
from numpy import array

# Load and parse the data
data = sc.textFile("mllib/data/sample_svm_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
model = LogisticRegressionWithSGD.train(parsedData)

# Build the model
labelsAndPreds = parsedData.map(lambda point: (int(point.item(0)),
        model.predict(point.take(range(1, point.size)))))

# Evaluating the model on training data
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))
{% endhighlight %}

## Linear Regression
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint.
The example then uses LinearRegressionWithSGD to build a simple linear model to predict label 
values. We compute the Mean Squared Error at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit)

{% highlight python %}
from pyspark.mllib.regression import LinearRegressionWithSGD
from numpy import array

# Load and parse the data
data = sc.textFile("mllib/data/ridge-data/lpsa.data")
parsedData = data.map(lambda line: array([float(x) for x in line.replace(',', ' ').split(' ')]))

# Build the model
model = LinearRegressionWithSGD.train(parsedData)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda point: (point.item(0),
        model.predict(point.take(range(1, point.size)))))
MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y)/valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))
{% endhighlight %}
