---
layout: global
title: MLlib - Clustering
---

* Table of contents
{:toc}


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



# Usage in Scala

Following code snippets can be executed in `spark-shell`.

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


# Usage in Java

All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object.

# Usage in Python
Following examples can be tested in the PySpark shell.

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

