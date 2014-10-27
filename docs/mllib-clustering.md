---
layout: global
title: Clustering - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Clustering
---

* Table of contents
{:toc}


## Clustering

Clustering is an unsupervised learning problem whereby we aim to group subsets
of entities with one another based on some notion of similarity.  Clustering is
often used for exploratory analysis and/or as a component of a hierarchical
supervised learning pipeline (in which distinct classifiers or regression
models are trained for each cluster). 


### K-means

MLlib supports
[k-means](http://en.wikipedia.org/wiki/K-means_clustering) clustering, one of
the most commonly used clustering algorithms that clusters the data points into
predefined number of clusters. The MLlib implementation includes a parallelized
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
* *initializationSteps* determines the number of steps in the k-means\|\| algorithm.
* *epsilon* determines the distance threshold within which we consider k-means to have converged. 


<div class="codetabs">
<div data-lang="scala" markdown="1">
The following code snippets can be executed in `spark-shell`.

In the following example after loading and parsing data, we use the
[`KMeans`](api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) object to cluster the data
into two clusters. The number of desired clusters is passed to the algorithm. We then compute Within
Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In fact the
optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

{% highlight scala %}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/kmeans_data.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. A self-contained application example
that is equivalent to the provided example in Scala is given below:

{% highlight java %}
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class KMeansExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("K-means Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load and parse data
    String path = "data/mllib/kmeans_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
      new Function<String, Vector>() {
        public Vector call(String s) {
          String[] sarray = s.split(" ");
          double[] values = new double[sarray.length];
          for (int i = 0; i < sarray.length; i++)
            values[i] = Double.parseDouble(sarray[i]);
          return Vectors.dense(values);
        }
      }
    );
    parsedData.cache();

    // Cluster the data into two classes using KMeans
    int numClusters = 2;
    int numIterations = 20;
    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    double WSSSE = clusters.computeCost(parsedData.rdd());
    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
The following examples can be tested in the PySpark shell.

In the following example after loading and parsing data, we use the KMeans object to cluster the
data into two clusters. The number of desired clusters is passed to the algorithm. We then compute
Within Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In
fact the optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

{% highlight python %}
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

# Load and parse the data
data = sc.textFile("data/mllib/kmeans_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 2, maxIterations=10,
        runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
{% endhighlight %}
</div>

</div>

In order to run the above application, follow the instructions
provided in the [Self-Contained Applications](quick-start.html#self-contained-applications)
section of the Spark
Quick Start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.


### Hierarchical Clustering

MLlib supports
[hierarchical clustering](http://en.wikipedia.org/wiki/Hierarchical_clustering), one of the most commonly used clustering algorithm which seeks to build a hierarchy of clusters.
Strategies for hierarchical clustering generally fall into two types.
One is the agglomerative clustering which is a "bottom up" approach: each observation starts in its own cluster, and pairs of clusters are merged as one moves up the hierarchy.
The other is the divisive clustering which is a "top down" approach: all observations start in one cluster, and splits are performed recursively as one moves down the hierarchy.
The MLlib implementation only includes a divisive hierarchical clustering algorithm.

The implementation in MLlib has the following parameters:

* *k* is the number of maximum desired clusters. 
* *subIterations* is the maximum number of iterations to split a cluster to its 2 sub clusters.
* *numRetries* is the maximum number of retries if a splitting doesn't work as expected.
* *epsilon* determines the saturate threshold to consider the splitting to have converged.



### Hierarchical Clustering Example

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippets can be executed in `spark-shell`.

In the following example after loading and parsing data, 
we use the hierarchical clustering object to cluster the sample data into three clusters. 
The number of desired clusters is passed to the algorithm. 
Hoerver, even though the number of clusters is less than *k* in the middle of the clustering,
the clustering is stopped if they can not be split any more.

{% highlight scala %}
import org.apache.spark.mllib.clustering.HierarchicalClustering
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/sample_hierarchical_data.csv")
val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

// Cluster the data into three classes using HierarchicalClustering object
val numClusters = 3
val model = HierarchicalClustering.train(parsedData, numClusters)

// Show the cluster centers
model.getCenters.foreach(println)

// Evaluate clustering by computing the sum of variance of the clusters
val variance = model.getClusters.map(_.getVariance.get).sum
println(s"Sum of Variance of the Clusters = ${variance}")
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. A self-contained application example
that is equivalent to the provided example in Scala is given below:

{% highlight java %}
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.HierarchicalClustering;
import org.apache.spark.mllib.clustering.HierarchicalClusteringModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaHierarchicalClustering {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Hierarchical Clustering Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load and parse data
        String path = "data/mllib/sample_hierarchical_data.csv";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
                new Function<String, Vector>() {
                    public Vector call(String s) {
                        String[] sarray = s.split(",");
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++)
                            values[i] = Double.parseDouble(sarray[i]);
                        return Vectors.dense(values);
                    }
                }
        );
        parsedData.cache();

        // Cluster the data into three classes using KMeans
        int numClusters = 3;
        HierarchicalClusteringModel model =
                HierarchicalClustering.train(parsedData.rdd(), numClusters);

        // Predict a point
        Vector vector = Vectors.dense(6.0, 3.0, 4.0, 1.0);
        int clusterIndex = model.predict(vector);
        System.out.println("Predicted the Closest Cluster Index: " + clusterIndex);

        // Evaluate clustering by computing total variance
        double variance = model.computeCost();
        System.out.println("Sum of Variance of the Clusters = " + variance);
    }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
The following examples can be tested in the PySpark shell.

In the following example after loading and parsing data, 
we use the HierarchicalClustering object to cluster the data into three clusters. 
The number of desired clusters is passed to the algorithm. 
We then compute Within Set Sum of Squared Error (WSSSE). 

{% highlight python %}
from pyspark.mllib.clustering import HierarchicalClustering
from numpy import array
from math import sqrt

# Load and parse the data
data = sc.textFile("./data/mllib/sample_hierarchical_data.csv")
parsedData = data.map(lambda line: array([float(x) for x in line.split(',')]))

# Build the model (cluster the data)
model = HierarchicalClustering.train(parsedData, 3)

# Get the cluster centers
model.clusterCenters

# Predict the index of cluster array 
point = [6, 3, 4, 1]
model.predict(point)

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = model.clusterCenters[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
{% endhighlight %}
</div>

</div>

## Streaming clustering

When data arrive in a stream, we may want to estimate clusters dynamically, 
updating them as new data arrive. MLlib provides support for streaming k-means clustering, 
with parameters to control the decay (or "forgetfulness") of the estimates. The algorithm 
uses a generalization of the mini-batch k-means update rule. For each batch of data, we assign 
all points to their nearest cluster, compute new cluster centers, then update each cluster using:

`\begin{equation}
    c_{t+1} = \frac{c_tn_t\alpha + x_tm_t}{n_t\alpha+m_t}
\end{equation}`
`\begin{equation}
    n_{t+1} = n_t + m_t  
\end{equation}`

Where `$c_t$` is the previous center for the cluster, `$n_t$` is the number of points assigned 
to the cluster thus far, `$x_t$` is the new cluster center from the current batch, and `$m_t$` 
is the number of points added to the cluster in the current batch. The decay factor `$\alpha$` 
can be used to ignore the past: with `$\alpha$=1` all data will be used from the beginning; 
with `$\alpha$=0` only the most recent data will be used. This is analogous to an 
exponentially-weighted moving average. 

The decay can be specified using a `halfLife` parameter, which determines the 
correct decay factor `a` such that, for data acquired
at time `t`, its contribution by time `t + halfLife` will have dropped to 0.5.
The unit of time can be specified either as `batches` or `points` and the update rule
will be adjusted accordingly.

### Examples

This example shows how to estimate clusters on streaming data.

<div class="codetabs">

<div data-lang="scala" markdown="1">

First we import the neccessary classes.

{% highlight scala %}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans

{% endhighlight %}

Then we make an input stream of vectors for training, as well as a stream of labeled data 
points for testing. We assume a StreamingContext `ssc` has been created, see 
[Spark Streaming Programming Guide](streaming-programming-guide.html#initializing) for more info.  

{% highlight scala %}

val trainingData = ssc.textFileStream("/training/data/dir").map(Vectors.parse)
val testData = ssc.textFileStream("/testing/data/dir").map(LabeledPoint.parse)

{% endhighlight %}

We create a model with random clusters and specify the number of clusters to find

{% highlight scala %}

val numDimensions = 3
val numClusters = 2
val model = new StreamingKMeans()
  .setK(numClusters)
  .setDecayFactor(1.0)
  .setRandomCenters(numDimensions, 0.0)

{% endhighlight %}

Now register the streams for training and testing and start the job, printing 
the predicted cluster assignments on new data points as they arrive.

{% highlight scala %}

model.trainOn(trainingData)
model.predictOnValues(testData).print()

ssc.start()
ssc.awaitTermination()
 
{% endhighlight %}

As you add new text files with data the cluster centers will update. Each training 
point should be formatted as `[x1, x2, x3]`, and each test data point
should be formatted as `(y, [x1, x2, x3])`, where `y` is some useful label or identifier 
(e.g. a true category assignment). Anytime a text file is placed in `/training/data/dir` 
the model will update. Anytime a text file is placed in `/testing/data/dir` 
you will see predictions. With new data, the cluster centers will change!

</div>

</div>
