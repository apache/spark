---
layout: global
title: MLlib - Classification and Regression
---

* Table of contents
{:toc}


# Binary Classification

Binary classification is a supervised learning problem in which we want to
classify entities into one of two distinct categories or labels, e.g.,
predicting whether or not emails are spam.  This problem involves executing a
learning *Algorithm* on a set of *labeled* examples, i.e., a set of entities
represented via (numerical) features along with underlying category labels.
The algorithm returns a trained *Model* that can predict the label for new
entities for which the underlying label is unknown. 
 
MLlib currently supports two standard model families for binary classification,
namely [Linear Support Vector Machines
(SVMs)](http://en.wikipedia.org/wiki/Support_vector_machine) and [Logistic
Regression](http://en.wikipedia.org/wiki/Logistic_regression), along with [L1
and L2 regularized](http://en.wikipedia.org/wiki/Regularization_(mathematics))
variants of each model family.  The training algorithms all leverage an
underlying gradient descent primitive (described
[below](#gradient-descent-primitive)), and take as input a regularization
parameter (*regParam*) along with various parameters associated with gradient
descent (*stepSize*, *numIterations*, *miniBatchFraction*). 

Available algorithms for binary classification:

* [SVMWithSGD](api/mllib/index.html#org.apache.spark.mllib.classification.SVMWithSGD)
* [LogisticRegressionWithSGD](api/mllib/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD)

# Linear Regression

Linear regression is another classical supervised learning setting.  In this
problem, each entity is associated with a real-valued label (as opposed to a
binary label as in binary classification), and we want to predict labels as
closely as possible given numerical features representing entities.  MLlib
supports linear regression as well as L1
([lasso](http://en.wikipedia.org/wiki/Lasso_(statistics)#Lasso_method)) and L2
([ridge](http://en.wikipedia.org/wiki/Ridge_regression)) regularized variants.
The regression algorithms in MLlib also leverage the underlying gradient
descent primitive (described [below](#gradient-descent-primitive)), and have
the same parameters as the binary classification algorithms described above. 

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
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint. The
example then uses LinearRegressionWithSGD to build a simple linear model to predict label values. We
compute the Mean Squared Error at the end to evaluate
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
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint. The
example then uses LinearRegressionWithSGD to build a simple linear model to predict label values. We
compute the Mean Squared Error at the end to evaluate
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
