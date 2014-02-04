---
layout: global
title: Machine Learning Library (MLlib)
---

* Table of contents
{:toc}

MLlib is a Spark implementation of some common machine learning (ML)
functionality, as well associated tests and data generators.  MLlib
currently supports four common types of machine learning problem settings,
namely, binary classification, regression, clustering and collaborative
filtering, as well as an underlying gradient descent optimization primitive.
This guide will outline the functionality supported in MLlib and also provides
an example of invoking MLlib.

# Dependencies
MLlib uses the [jblas](https://github.com/mikiobraun/jblas) linear algebra library, which itself
depends on native Fortran routines. You may need to install the 
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes. MLlib will throw a linking error if it cannot 
detect these libraries automatically.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.7 or newer
and Python 2.7.

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

# Clustering

Clustering is an unsupervised learning problem whereby we aim to group subsets
of entities with one another based on some notion of similarity.  Clustering is
often used for exploratory analysis and/or as a component of a hierarchical
supervised learning pipeline (in which distinct classifiers or regression
models are trained for each cluster). MLlib supports
[k-means](http://en.wikipedia.org/wiki/K-means_clustering) clustering, one of
the most commonly used clustering algorithms that clusters the data points into
predfined number of clusters. The MLlib implementation includes a parallelized
variant of the [k-means++](http://en.wikipedia.org/wiki/K-means%2B%2B) method
called [kmeans||](http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf).
The implementation in MLlib has the following parameters:  

* *k* is the number of desired clusters.
* *maxIterations* is the maximum number of iterations to run.
* *initializationMode* specifies either random initialization or
initialization via k-means\|\|.
* *runs* is the number of times to run the k-means algorithm (k-means is not
guaranteed to find a globally optimal solution, and when run multiple times on
a given dataset, the algorithm returns the best clustering result).
* *initializiationSteps* determines the number of steps in the k-means\|\| algorithm.
* *epsilon* determines the distance threshold within which we consider k-means to have converged. 

Available algorithms for clustering: 

* [KMeans](api/mllib/index.html#org.apache.spark.mllib.clustering.KMeans)

# Collaborative Filtering 

[Collaborative filtering](http://en.wikipedia.org/wiki/Recommender_system#Collaborative_filtering)
is commonly used for recommender systems.  These techniques aim to fill in the
missing entries of a user-item association matrix.  MLlib currently supports
model-based collaborative filtering, in which users and products are described
by a small set of latent factors that can be used to predict missing entries.
In particular, we implement the [alternating least squares
(ALS)](http://www2.research.att.com/~volinsky/papers/ieeecomputer.pdf)
algorithm to learn these latent factors. The implementation in MLlib has the
following parameters:

* *numBlocks* is the number of blacks used to parallelize computation (set to -1 to auto-configure). 
* *rank* is the number of latent factors in our model.
* *iterations* is the number of iterations to run.
* *lambda* specifies the regularization parameter in ALS.
* *implicitPrefs* specifies whether to use the *explicit feedback* ALS variant or one adapted for *implicit feedback* data
* *alpha* is a parameter applicable to the implicit feedback variant of ALS that governs the *baseline* confidence in preference observations

## Explicit vs Implicit Feedback

The standard approach to matrix factorization based collaborative filtering treats 
the entries in the user-item matrix as *explicit* preferences given by the user to the item.

It is common in many real-world use cases to only have access to *implicit feedback* 
(e.g. views, clicks, purchases, likes, shares etc.). The approach used in MLlib to deal with 
such data is taken from 
[Collaborative Filtering for Implicit Feedback Datasets](http://www2.research.att.com/~yifanhu/PUB/cf.pdf).
Essentially instead of trying to model the matrix of ratings directly, this approach treats the data as 
a combination of binary preferences and *confidence values*. The ratings are then related 
to the level of confidence in observed user preferences, rather than explicit ratings given to items. 
The model then tries to find latent factors that can be used to predict the expected preference of a user
for an item. 

Available algorithms for collaborative filtering: 

* [ALS](api/mllib/index.html#org.apache.spark.mllib.recommendation.ALS)

# Gradient Descent Primitive

[Gradient descent](http://en.wikipedia.org/wiki/Gradient_descent) (along with
stochastic variants thereof) are first-order optimization methods that are
well-suited for large-scale and distributed computation. Gradient descent
methods aim to find a local minimum of a function by iteratively taking steps
in the direction of the negative gradient of the function at the current point,
i.e., the current parameter value. Gradient descent is included as a low-level
primitive in MLlib, upon which various ML algorithms are developed, and has the
following parameters:

* *gradient* is a class that computes the stochastic gradient of the function
being optimized, i.e., with respect to a single training example, at the
current parameter value. MLlib includes gradient classes for common loss
functions, e.g., hinge, logistic, least-squares.  The gradient class takes as
input a training example, its label, and the current parameter value. 
* *updater* is a class that updates weights in each iteration of gradient
descent. MLlib includes updaters for cases without regularization, as well as
L1 and L2 regularizers.
* *stepSize* is a scalar value denoting the initial step size for gradient
descent. All updaters in MLlib use a step size at the t-th step equal to
stepSize / sqrt(t). 
* *numIterations* is the number of iterations to run.
* *regParam* is the regularization parameter when using L1 or L2 regularization.
* *miniBatchFraction* is the fraction of the data used to compute the gradient
at each iteration.

Available algorithms for gradient descent:

* [GradientDescent](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent)

# Using MLLib in Scala

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

## Clustering
In the following example after loading and parsing data, we use the KMeans object to cluster the data
into two clusters. The number of desired clusters is passed to the algorithm. We then compute Within
Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In fact the
optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

{% highlight scala %}
import org.apache.spark.mllib.clustering.KMeans

// Load and parse the data
val data = sc.textFile("kmeans_data.txt")
val parsedData = data.map( _.split(' ').map(_.toDouble))

// Cluster the data into two classes using KMeans
val numIterations = 20
val numClusters = 2
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)
{% endhighlight %}


## Collaborative Filtering
In the following example we load rating data. Each row consists of a user, a product and a rating.
We use the default ALS.train() method which assumes ratings are explicit. We evaluate the recommendation
model by measuring the Mean Squared Error of rating prediction.

{% highlight scala %}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

// Load and parse the data
val data = sc.textFile("mllib/data/als/test.data")
val ratings = data.map(_.split(',') match {
    case Array(user, item, rate) =>  Rating(user.toInt, item.toInt, rate.toDouble)
})

// Build the recommendation model using ALS
val numIterations = 20
val model = ALS.train(ratings, 1, 20, 0.01)

// Evaluate the model on rating data
val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
val predictions = model.predict(usersProducts).map{
    case Rating(user, product, rate) => ((user, product), rate)
}
val ratesAndPreds = ratings.map{
    case Rating(user, product, rate) => ((user, product), rate)
}.join(predictions)
val MSE = ratesAndPreds.map{
    case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
}.reduce(_ + _)/ratesAndPreds.count
println("Mean Squared Error = " + MSE)
{% endhighlight %}

If the rating matrix is derived from other source of information (i.e., it is inferred from
other signals), you can use the trainImplicit method to get better results.

{% highlight scala %}
val model = ALS.trainImplicit(ratings, 1, 20, 0.01)
{% endhighlight %}

# Using MLLib in Java

All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object.

# Using MLLib in Python
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


## Clustering
In the following example after loading and parsing data, we use the KMeans object to cluster the data
into two clusters. The number of desired clusters is passed to the algorithm. We then compute Within
Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In fact the
optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

{% highlight python %}
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

# Load and parse the data
data = sc.textFile("kmeans_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 2, maxIterations=10,
        runs=30, initialization_mode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
{% endhighlight %}

Similarly you can use RidgeRegressionWithSGD and LassoWithSGD and compare training Mean Squared
Errors.

## Collaborative Filtering
In the following example we load rating data. Each row consists of a user, a product and a rating.
We use the default ALS.train() method which assumes ratings are explicit. We evaluate the
recommendation by measuring the Mean Squared Error of rating prediction.

{% highlight python %}
from pyspark.mllib.recommendation import ALS
from numpy import array

# Load and parse the data
data = sc.textFile("mllib/data/als/test.data")
ratings = data.map(lambda line: array([float(x) for x in line.split(',')]))

# Build the recommendation model using Alternating Least Squares
model = ALS.train(ratings, 1, 20)

# Evaluate the model on training data
testdata = ratings.map(lambda p: (int(p[0]), int(p[1])))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/ratesAndPreds.count()
print("Mean Squared Error = " + str(MSE))
{% endhighlight %}

If the rating matrix is derived from other source of information (i.e., it is inferred from other
signals), you can use the trainImplicit method to get better results.

{% highlight python %}
# Build the recommendation model using Alternating Least Squares based on implicit ratings
model = ALS.trainImplicit(ratings, 1, 20)
{% endhighlight %}


# Singular Value Decomposition
Singular Value Decomposition for Tall and Skinny matrices.
Given an *m x n* matrix *A*, we can compute matrices *U, S, V* such that

*A = U * S * V^T*

There is no restriction on m, but we require n^2 doubles to
fit in memory locally on one machine.
Further, n should be less than m.

The decomposition is computed by first computing *A^TA = V S^2 V^T*,
computing SVD locally on that (since n x n is small),
from which we recover S and V.
Then we compute U via easy matrix multiplication
as *U =  A * V * S^-1*

Only singular vectors associated with largest k singular values
are recovered. If there are k
such values, then the dimensions of the return will be:

* *S* is *k x k* and diagonal, holding the singular values on diagonal.
* *U* is *m x k* and satisfies U^T*U = eye(k).
* *V* is *n x k* and satisfies V^TV = eye(k).

All input and output is expected in sparse matrix format, 0-indexed
as tuples of the form ((i,j),value) all in
SparseMatrix RDDs. Below is example usage.

{% highlight scala %}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SVD
import org.apache.spark.mllib.linalg.SparseMatrix
import org.apache.spark.mllib.linalg.MatrixEntry

// Load and parse the data file
val data = sc.textFile("mllib/data/als/test.data").map { line =>
  val parts = line.split(',')
  MatrixEntry(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
}
val m = 4
val n = 4
val k = 1

// recover largest singular vector
val decomposed = SVD.sparseSVD(SparseMatrix(data, m, n), k)
val = decomposed.S.data

println("singular values = " + s.toArray.mkString)
{% endhighlight %}