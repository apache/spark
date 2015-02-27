---
layout: global
title: Linear Methods - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Linear Methods
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

## Mathematical formulation

Many standard *machine learning* methods can be formulated as a convex optimization problem, i.e.
the task of finding a minimizer of a convex function `$f$` that depends on a variable vector
`$\wv$` (called `weights` in the code), which has `$d$` entries. 
Formally, we can write this as the optimization problem `$\min_{\wv \in\R^d} \; f(\wv)$`, where
the objective function is of the form
`\begin{equation}
    f(\wv) := \lambda\, R(\wv) +
    \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i)
    \label{eq:regPrimal}
    \ .
\end{equation}`
Here the vectors `$\x_i\in\R^d$` are the training data examples, for `$1\le i\le n$`, and
`$y_i\in\R$` are their corresponding labels, which we want to predict. 
We call the method *linear* if $L(\wv; \x, y)$ can be expressed as a function of $\wv^T x$ and $y$.
Several of MLlib's classification and regression algorithms fall into this category,
and are discussed here.

The objective function `$f$` has two parts:
the regularizer that controls the complexity of the model,
and the loss that measures the error of the model on the training data.
The loss function `$L(\wv;.)$` is typically a convex function in `$\wv$`.  The
fixed regularization parameter `$\lambda \ge 0$` (`regParam` in the code)
defines the trade-off between the two goals of minimizing the loss (i.e.,
training error) and minimizing model complexity (i.e., to avoid overfitting).

### Loss functions

The following table summarizes the loss functions and their gradients or sub-gradients for the
methods MLlib supports:

<table class="table">
  <thead>
    <tr><th></th><th>loss function $L(\wv; \x, y)$</th><th>gradient or sub-gradient</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>hinge loss</td><td>$\max \{0, 1-y \wv^T \x \}, \quad y \in \{-1, +1\}$</td>
      <td>$\begin{cases}-y \cdot \x &amp; \text{if $y \wv^T \x &lt;1$}, \\ 0 &amp;
\text{otherwise}.\end{cases}$</td>
    </tr>
    <tr>
      <td>logistic loss</td><td>$\log(1+\exp( -y \wv^T \x)), \quad y \in \{-1, +1\}$</td>
      <td>$-y \left(1-\frac1{1+\exp(-y \wv^T \x)} \right) \cdot \x$</td>
    </tr>
    <tr>
      <td>squared loss</td><td>$\frac{1}{2} (\wv^T \x - y)^2, \quad y \in \R$</td>
      <td>$(\wv^T \x - y) \cdot \x$</td>
    </tr>
  </tbody>
</table>

### Regularizers

The purpose of the
[regularizer](http://en.wikipedia.org/wiki/Regularization_(mathematics)) is to
encourage simple models and avoid overfitting.  We support the following
regularizers in MLlib:

<table class="table">
  <thead>
    <tr><th></th><th>regularizer $R(\wv)$</th><th>gradient or sub-gradient</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>zero (unregularized)</td><td>0</td><td>$\0$</td>
    </tr>
    <tr>
      <td>L2</td><td>$\frac{1}{2}\|\wv\|_2^2$</td><td>$\wv$</td>
    </tr>
    <tr>
      <td>L1</td><td>$\|\wv\|_1$</td><td>$\mathrm{sign}(\wv)$</td>
    </tr>
  </tbody>
</table>

Here `$\mathrm{sign}(\wv)$` is the vector consisting of the signs (`$\pm1$`) of all the entries
of `$\wv$`.

L2-regularized problems are generally easier to solve than L1-regularized due to smoothness.
However, L1 regularization can help promote sparsity in weights leading to smaller and more interpretable models, the latter of which can be useful for feature selection.
It is not recommended to train models without any regularization,
especially when the number of training examples is small.

### Optimization

Under the hood, linear methods use convex optimization methods to optimize the objective functions.  MLlib uses two methods, SGD and L-BFGS, described in the [optimization section](mllib-optimization.html).  Currently, most algorithm APIs support Stochastic Gradient Descent (SGD), and a few support L-BFGS. Refer to [this optimization section](mllib-optimization.html#Choosing-an-Optimization-Method) for guidelines on choosing between optimization methods.

## Binary classification

[Binary classification](http://en.wikipedia.org/wiki/Binary_classification)
aims to divide items into two categories: positive and negative.  MLlib
supports two linear methods for binary classification: linear Support Vector
Machines (SVMs) and logistic regression. For both methods, MLlib supports
L1 and L2 regularized variants. The training data set is represented by an RDD
of [LabeledPoint](mllib-data-types.html) in MLlib.  Note that, in the
mathematical formulation in this guide, a training label $y$ is denoted as
either $+1$ (positive) or $-1$ (negative), which is convenient for the
formulation.  *However*, the negative label is represented by $0$ in MLlib
instead of $-1$, to be consistent with multiclass labeling.

### Linear Support Vector Machines (SVMs)

The [linear SVM](http://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM)
is a standard method for large-scale classification tasks. It is a linear method as described above in equation `$\eqref{eq:regPrimal}$`, with the loss function in the formulation given by the hinge loss:

`\[
L(\wv;\x,y) := \max \{0, 1-y \wv^T \x \}.
\]`
By default, linear SVMs are trained with an L2 regularization.
We also support alternative L1 regularization. In this case,
the problem becomes a [linear program](http://en.wikipedia.org/wiki/Linear_programming).

The linear SVMs algorithm outputs an SVM model. Given a new data point,
denoted by $\x$, the model makes predictions based on the value of $\wv^T \x$.
By the default, if $\wv^T \x \geq 0$ then the outcome is positive, and negative
otherwise.

### Logistic regression

[Logistic regression](http://en.wikipedia.org/wiki/Logistic_regression) is widely used to predict a
binary response. 
It is a linear method as described above in equation `$\eqref{eq:regPrimal}$`, with the loss
function in the formulation given by the logistic loss:
`\[
L(\wv;\x,y) :=  \log(1+\exp( -y \wv^T \x)).
\]`

The logistic regression algorithm outputs a logistic regression model.  Given a
new data point, denoted by $\x$, the model makes predictions by
applying the logistic function
`\[
\mathrm{f}(z) = \frac{1}{1 + e^{-z}}
\]`
where $z = \wv^T \x$.
By default, if $\mathrm{f}(\wv^T x) > 0.5$, the outcome is positive, or
negative otherwise, though unlike linear SVMs, the raw output of the logistic regression
model, $\mathrm{f}(z)$, has a probabilistic interpretation (i.e., the probability
that $\x$ is positive).

### Evaluation metrics

MLlib supports common evaluation metrics for binary classification (not available in PySpark). 
This
includes precision, recall, [F-measure](http://en.wikipedia.org/wiki/F1_score),
[receiver operating characteristic (ROC)](http://en.wikipedia.org/wiki/Receiver_operating_characteristic),
precision-recall curve, and
[area under the curves (AUC)](http://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).
AUC is commonly used to compare the performance of various models while
precision/recall/F-measure can help determine the appropriate threshold to use
for prediction purposes. 

### Examples

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippet illustrates how to load a sample dataset, execute a
training algorithm on this training data using a static method in the algorithm
object, and make predictions with the resulting model to compute the training
error.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val numIterations = 100
val model = SVMWithSGD.train(training, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set. 
val scoreAndLabels = test.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Area under ROC = " + auROC)

// Save and load model
model.save(sc, "myModelPath")
val sameModel = SVMModel.load(sc, "myModelPath")
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
svmAlg.optimizer.
  setNumIterations(200).
  setRegParam(0.1).
  setUpdater(new L1Updater)
val modelL1 = svmAlg.run(training)
{% endhighlight %}

[`LogisticRegressionWithSGD`](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD) can be used in a similar fashion as `SVMWithSGD`.

</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. A self-contained application example
that is equivalent to the provided example in Scala is given bellow:

{% highlight java %}
import java.util.Random;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.*;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SVMClassifier {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SVM Classifier Example");
    SparkContext sc = new SparkContext(conf);
    String path = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
    training.cache();
    JavaRDD<LabeledPoint> test = data.subtract(training);
    
    // Run training algorithm to build the model.
    int numIterations = 100;
    final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);
    
    // Clear the default threshold.
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double score = model.predict(p.features());
          return new Tuple2<Object, Object>(score, p.label());
        }
      }
    );
    
    // Get evaluation metrics.
    BinaryClassificationMetrics metrics = 
      new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
    double auROC = metrics.areaUnderROC();
    
    System.out.println("Area under ROC = " + auROC);

    // Save and load model
    model.save(sc.sc(), "myModelPath");
    SVMModel sameModel = SVMModel.load(sc.sc(), "myModelPath");
  }
}
{% endhighlight %}

The `SVMWithSGD.train()` method by default performs L2 regularization with the
regularization parameter set to 1.0. If we want to configure this algorithm, we
can customize `SVMWithSGD` further by creating a new object directly and
calling setter methods. All other MLlib algorithms support customization in
this way as well. For example, the following code produces an L1 regularized
variant of SVMs with regularization parameter set to 0.1, and runs the training
algorithm for 200 iterations.

{% highlight java %}
import org.apache.spark.mllib.optimization.L1Updater;

SVMWithSGD svmAlg = new SVMWithSGD();
svmAlg.optimizer()
  .setNumIterations(200)
  .setRegParam(0.1)
  .setUpdater(new L1Updater());
final SVMModel modelL1 = svmAlg.run(training.rdd());
{% endhighlight %}

In order to run the above application, follow the instructions
provided in the [Self-Contained
Applications](quick-start.html#self-contained-applications) section of the Spark
quick-start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.
</div>

<div data-lang="python" markdown="1">
The following example shows how to load a sample dataset, build Logistic Regression model,
and make predictions with the resulting model to compute the training error.

Note that the Python API does not yet support model save/load but will in the future.

{% highlight python %}
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from numpy import array

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("data/mllib/sample_svm_data.txt")
parsedData = data.map(parsePoint)

# Build the model
model = LogisticRegressionWithSGD.train(parsedData)

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))
{% endhighlight %}
</div>
</div>

## Linear least squares, Lasso, and ridge regression


Linear least squares is the most common formulation for regression problems. 
It is a linear method as described above in equation `$\eqref{eq:regPrimal}$`, with the loss
function in the formulation given by the squared loss:
`\[
L(\wv;\x,y) :=  \frac{1}{2} (\wv^T \x - y)^2.
\]`

Various related regression methods are derived by using different types of regularization:
[*ordinary least squares*](http://en.wikipedia.org/wiki/Ordinary_least_squares) or 
[*linear least squares*](http://en.wikipedia.org/wiki/Linear_least_squares_(mathematics)) uses 
 no regularization; [*ridge regression*](http://en.wikipedia.org/wiki/Ridge_regression) uses L2
regularization; and [*Lasso*](http://en.wikipedia.org/wiki/Lasso_(statistics)) uses L1
regularization.  For all of these models, the average loss or training error, $\frac{1}{n} \sum_{i=1}^n (\wv^T x_i - y_i)^2$, is
known as the [mean squared error](http://en.wikipedia.org/wiki/Mean_squared_error).

### Examples

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint.
The example then uses LinearRegressionWithSGD to build a simple linear model to predict label 
values. We compute the mean squared error at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

{% highlight scala %}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}.cache()

// Building the model
val numIterations = 100
val model = LinearRegressionWithSGD.train(parsedData, numIterations)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
println("training Mean Squared Error = " + MSE)

// Save and load model
model.save(sc, "myModelPath")
val sameModel = LinearRegressionModel.load(sc, "myModelPath")
{% endhighlight %}

[`RidgeRegressionWithSGD`](api/scala/index.html#org.apache.spark.mllib.regression.RidgeRegressionWithSGD)
and [`LassoWithSGD`](api/scala/index.html#org.apache.spark.mllib.regression.LassoWithSGD) can be used in a similar fashion as `LinearRegressionWithSGD`.

</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. The corresponding Java example to
the Scala snippet provided, is presented bellow:

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.SparkConf;

public class LinearRegression {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Linear Regression Example");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    // Load and parse the data
    String path = "data/mllib/ridge-data/lpsa.data";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<LabeledPoint> parsedData = data.map(
      new Function<String, LabeledPoint>() {
        public LabeledPoint call(String line) {
          String[] parts = line.split(",");
          String[] features = parts[1].split(" ");
          double[] v = new double[features.length];
          for (int i = 0; i < features.length - 1; i++)
            v[i] = Double.parseDouble(features[i]);
          return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
        }
      }
    );
    parsedData.cache();

    // Building the model
    int numIterations = 100;
    final LinearRegressionModel model = 
      LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

    // Evaluate model on training examples and compute training error
    JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
      new Function<LabeledPoint, Tuple2<Double, Double>>() {
        public Tuple2<Double, Double> call(LabeledPoint point) {
          double prediction = model.predict(point.features());
          return new Tuple2<Double, Double>(prediction, point.label());
        }
      }
    );
    double MSE = new JavaDoubleRDD(valuesAndPreds.map(
      new Function<Tuple2<Double, Double>, Object>() {
        public Object call(Tuple2<Double, Double> pair) {
          return Math.pow(pair._1() - pair._2(), 2.0);
        }
      }
    ).rdd()).mean();
    System.out.println("training Mean Squared Error = " + MSE);

    // Save and load model
    model.save(sc.sc(), "myModelPath");
    LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModelPath");
  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint.
The example then uses LinearRegressionWithSGD to build a simple linear model to predict label 
values. We compute the mean squared error at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

Note that the Python API does not yet support model save/load but will in the future.

{% highlight python %}
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from numpy import array

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("data/mllib/ridge-data/lpsa.data")
parsedData = data.map(parsePoint)

# Build the model
model = LinearRegressionWithSGD.train(parsedData)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))
{% endhighlight %}
</div>
</div>

In order to run the above application, follow the instructions
provided in the [Self-Contained Applications](quick-start.html#self-contained-applications)
section of the Spark
quick-start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.

## Streaming linear regression

When data arrive in a streaming fashion, it is useful to fit regression models online, 
updating the parameters of the model as new data arrives. MLlib currently supports 
streaming linear regression using ordinary least squares. The fitting is similar
to that performed offline, except fitting occurs on each batch of data, so that
the model continually updates to reflect the data from the stream.

### Examples

The following example demonstrates how to load training and testing data from two different
input streams of text files, parse the streams as labeled points, fit a linear regression model
online to the first stream, and make predictions on the second stream.

<div class="codetabs">

<div data-lang="scala" markdown="1">

First, we import the necessary classes for parsing our input data and creating the model. 

{% highlight scala %}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD

{% endhighlight %}

Then we make input streams for training and testing data. We assume a StreamingContext `ssc`
has already been created, see [Spark Streaming Programming Guide](streaming-programming-guide.html#initializing)
for more info. For this example, we use labeled points in training and testing streams, 
but in practice you will likely want to use unlabeled vectors for test data.

{% highlight scala %}

val trainingData = ssc.textFileStream("/training/data/dir").map(LabeledPoint.parse).cache()
val testData = ssc.textFileStream("/testing/data/dir").map(LabeledPoint.parse)

{% endhighlight %}

We create our model by initializing the weights to 0

{% highlight scala %}

val numFeatures = 3
val model = new StreamingLinearRegressionWithSGD()
    .setInitialWeights(Vectors.zeros(numFeatures))

{% endhighlight %}

Now we register the streams for training and testing and start the job. 
Printing predictions alongside true labels lets us easily see the result.

{% highlight scala %}

model.trainOn(trainingData)
model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

ssc.start()
ssc.awaitTermination()
 
{% endhighlight %}

We can now save text files with data to the training or testing folders.
Each line should be a data point formatted as `(y,[x1,x2,x3])` where `y` is the label 
and `x1,x2,x3` are the features. Anytime a text file is placed in `/training/data/dir` 
the model will update. Anytime a text file is placed in `/testing/data/dir` you will see predictions. 
As you feed more data to the training directory, the predictions 
will get better!

</div>

</div>


## Implementation (developer)

Behind the scene, MLlib implements a simple distributed version of stochastic gradient descent
(SGD), building on the underlying gradient descent primitive (as described in the <a
href="mllib-optimization.html">optimization</a> section).  All provided algorithms take as input a
regularization parameter (`regParam`) along with various parameters associated with stochastic
gradient descent (`stepSize`, `numIterations`, `miniBatchFraction`).  For each of them, we support
all three possible regularizations (none, L1 or L2).

Algorithms are all implemented in Scala:

* [SVMWithSGD](api/scala/index.html#org.apache.spark.mllib.classification.SVMWithSGD)
* [LogisticRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD)
* [LinearRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.regression.LinearRegressionWithSGD)
* [RidgeRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.regression.RidgeRegressionWithSGD)
* [LassoWithSGD](api/scala/index.html#org.apache.spark.mllib.regression.LassoWithSGD)

Python calls the Scala implementation via
[PythonMLLibAPI](api/scala/index.html#org.apache.spark.mllib.api.python.PythonMLLibAPI).
