---
layout: global
title: Clustering - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Clustering
---

Clustering is an unsupervised learning problem whereby we aim to group subsets
of entities with one another based on some notion of similarity.  Clustering is
often used for exploratory analysis and/or as a component of a hierarchical
supervised learning pipeline (in which distinct classifiers or regression
models are trained for each cluster).

MLlib supports the following models:

* Table of contents
{:toc}

## K-means

[k-means](http://en.wikipedia.org/wiki/K-means_clustering) is one of the
most commonly used clustering algorithms that clusters the data points into a
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

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">
The following code snippets can be executed in `spark-shell`.

In the following example after loading and parsing data, we use the
[`KMeans`](api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) object to cluster the data
into two clusters. The number of desired clusters is passed to the algorithm. We then compute Within
Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In fact the
optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

{% highlight scala %}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
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

// Save and load model
clusters.save(sc, "myModelPath")
val sameModel = KMeansModel.load(sc, "myModelPath")
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

    // Save and load model
    clusters.save(sc.sc(), "myModelPath");
    KMeansModel sameModel = KMeansModel.load(sc.sc(), "myModelPath");
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
from pyspark.mllib.clustering import KMeans, KMeansModel
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

# Save and load model
clusters.save(sc, "myModelPath")
sameModel = KMeansModel.load(sc, "myModelPath")
{% endhighlight %}
</div>

</div>

## Gaussian mixture

A [Gaussian Mixture Model](http://en.wikipedia.org/wiki/Mixture_model#Multivariate_Gaussian_mixture_model)
represents a composite distribution whereby points are drawn from one of *k* Gaussian sub-distributions,
each with its own probability.  The MLlib implementation uses the
[expectation-maximization](http://en.wikipedia.org/wiki/Expectation%E2%80%93maximization_algorithm)
 algorithm to induce the maximum-likelihood model given a set of samples.  The implementation
has the following parameters:

* *k* is the number of desired clusters.
* *convergenceTol* is the maximum change in log-likelihood at which we consider convergence achieved.
* *maxIterations* is the maximum number of iterations to perform without reaching convergence.
* *initialModel* is an optional starting point from which to start the EM algorithm. If this parameter is omitted, a random starting point will be constructed from the data.

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">
In the following example after loading and parsing data, we use a
[GaussianMixture](api/scala/index.html#org.apache.spark.mllib.clustering.GaussianMixture)
object to cluster the data into two clusters. The number of desired clusters is passed
to the algorithm. We then output the parameters of the mixture model.

{% highlight scala %}
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/gmm_data.txt")
val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()

// Cluster the data into two classes using GaussianMixture
val gmm = new GaussianMixture().setK(2).run(parsedData)

// Save and load model
gmm.save(sc, "myGMMModel")
val sameModel = GaussianMixtureModel.load(sc, "myGMMModel")

// output parameters of max-likelihood model
for (i <- 0 until gmm.k) {
  println("weight=%f\nmu=%s\nsigma=\n%s\n" format
    (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
}

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
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class GaussianMixtureExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("GaussianMixture Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load and parse data
    String path = "data/mllib/gmm_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
      new Function<String, Vector>() {
        public Vector call(String s) {
          String[] sarray = s.trim().split(" ");
          double[] values = new double[sarray.length];
          for (int i = 0; i < sarray.length; i++)
            values[i] = Double.parseDouble(sarray[i]);
          return Vectors.dense(values);
        }
      }
    );
    parsedData.cache();

    // Cluster the data into two classes using GaussianMixture
    GaussianMixtureModel gmm = new GaussianMixture().setK(2).run(parsedData.rdd());

    // Save and load GaussianMixtureModel
    gmm.save(sc.sc(), "myGMMModel");
    GaussianMixtureModel sameModel = GaussianMixtureModel.load(sc.sc(), "myGMMModel");
    // Output the parameters of the mixture model
    for(int j=0; j<gmm.k(); j++) {
        System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n",
            gmm.weights()[j], gmm.gaussians()[j].mu(), gmm.gaussians()[j].sigma());
    }
  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
In the following example after loading and parsing data, we use a
[GaussianMixture](api/python/pyspark.mllib.html#pyspark.mllib.clustering.GaussianMixture)
object to cluster the data into two clusters. The number of desired clusters is passed
to the algorithm. We then output the parameters of the mixture model.

{% highlight python %}
from pyspark.mllib.clustering import GaussianMixture
from numpy import array

# Load and parse the data
data = sc.textFile("data/mllib/gmm_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.strip().split(' ')]))

# Build the model (cluster the data)
gmm = GaussianMixture.train(parsedData, 2)

# output parameters of model
for i in range(2):
    print ("weight = ", gmm.weights[i], "mu = ", gmm.gaussians[i].mu,
        "sigma = ", gmm.gaussians[i].sigma.toArray())

{% endhighlight %}
</div>

</div>

## Power iteration clustering (PIC)

Power iteration clustering (PIC) is a scalable and efficient algorithm for clustering vertices of a
graph given pairwise similarties as edge properties,
described in [Lin and Cohen, Power Iteration Clustering](http://www.icml2010.org/papers/387.pdf).
It computes a pseudo-eigenvector of the normalized affinity matrix of the graph via
[power iteration](http://en.wikipedia.org/wiki/Power_iteration)  and uses it to cluster vertices.
MLlib includes an implementation of PIC using GraphX as its backend.
It takes an `RDD` of `(srcId, dstId, similarity)` tuples and outputs a model with the clustering assignments.
The similarities must be nonnegative.
PIC assumes that the similarity measure is symmetric.
A pair `(srcId, dstId)` regardless of the ordering should appear at most once in the input data.
If a pair is missing from input, their similarity is treated as zero.
MLlib's PIC implementation takes the following (hyper-)parameters:

* `k`: number of clusters
* `maxIterations`: maximum number of power iterations
* `initializationMode`: initialization model. This can be either "random", which is the default,
  to use a random vector as vertex properties, or "degree" to use normalized sum similarities.

**Examples**

In the following, we show code snippets to demonstrate how to use PIC in MLlib.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`PowerIterationClustering`](api/scala/index.html#org.apache.spark.mllib.clustering.PowerIterationClustering) 
implements the PIC algorithm.
It takes an `RDD` of `(srcId: Long, dstId: Long, similarity: Double)` tuples representing the
affinity matrix.
Calling `PowerIterationClustering.run` returns a
[`PowerIterationClusteringModel`](api/scala/index.html#org.apache.spark.mllib.clustering.PowerIterationClusteringModel),
which contains the computed clustering assignments.

{% highlight scala %}
import org.apache.spark.mllib.clustering.{PowerIterationClustering, PowerIterationClusteringModel}
import org.apache.spark.mllib.linalg.Vectors

val similarities: RDD[(Long, Long, Double)] = ...

val pic = new PowerIterationClustering()
  .setK(3)
  .setMaxIterations(20)
val model = pic.run(similarities)

model.assignments.foreach { a =>
  println(s"${a.id} -> ${a.cluster}")
}

// Save and load model
model.save(sc, "myModelPath")
val sameModel = PowerIterationClusteringModel.load(sc, "myModelPath")
{% endhighlight %}

A full example that produces the experiment described in the PIC paper can be found under
[`examples/`](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/PowerIterationClusteringExample.scala).

</div>

<div data-lang="java" markdown="1">

[`PowerIterationClustering`](api/java/org/apache/spark/mllib/clustering/PowerIterationClustering.html)
implements the PIC algorithm.
It takes an `JavaRDD` of `(srcId: Long, dstId: Long, similarity: Double)` tuples representing the
affinity matrix.
Calling `PowerIterationClustering.run` returns a
[`PowerIterationClusteringModel`](api/java/org/apache/spark/mllib/clustering/PowerIterationClusteringModel.html)
which contains the computed clustering assignments.

{% highlight java %}
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;

JavaRDD<Tuple3<Long, Long, Double>> similarities = ...

PowerIterationClustering pic = new PowerIterationClustering()
  .setK(2)
  .setMaxIterations(10);
PowerIterationClusteringModel model = pic.run(similarities);

for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
  System.out.println(a.id() + " -> " + a.cluster());
}

// Save and load model
model.save(sc.sc(), "myModelPath");
PowerIterationClusteringModel sameModel = PowerIterationClusteringModel.load(sc.sc(), "myModelPath");
{% endhighlight %}
</div>

</div>

## Latent Dirichlet allocation (LDA)

[Latent Dirichlet allocation (LDA)](http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)
is a topic model which infers topics from a collection of text documents.
LDA can be thought of as a clustering algorithm as follows:

* Topics correspond to cluster centers, and documents correspond to examples (rows) in a dataset.
* Topics and documents both exist in a feature space, where feature vectors are vectors of word counts.
* Rather than estimating a clustering using a traditional distance, LDA uses a function based
 on a statistical model of how text documents are generated.

LDA takes in a collection of documents as vectors of word counts.
It supports different inference algorithms via `setOptimizer` function. EMLDAOptimizer learns clustering using [expectation-maximization](http://en.wikipedia.org/wiki/Expectation%E2%80%93maximization_algorithm)
on the likelihood function and yields comprehensive results, while OnlineLDAOptimizer uses iterative mini-batch sampling for [online variational inference](https://www.cs.princeton.edu/~blei/papers/HoffmanBleiBach2010b.pdf) and is generally memory friendly. After fitting on the documents, LDA provides:

* Topics: Inferred topics, each of which is a probability distribution over terms (words).
* Topic distributions for documents: For each document in the training set, LDA gives a probability distribution over topics. (EM only)

LDA takes the following parameters:

* `k`: Number of topics (i.e., cluster centers)
* `maxIterations`: Limit on the number of iterations of EM used for learning
* `docConcentration`: Hyperparameter for prior over documents' distributions over topics. Currently must be > 1, where larger values encourage smoother inferred distributions.
* `topicConcentration`: Hyperparameter for prior over topics' distributions over terms (words). Currently must be > 1, where larger values encourage smoother inferred distributions.
* `checkpointInterval`: If using checkpointing (set in the Spark configuration), this parameter specifies the frequency with which checkpoints will be created.  If `maxIterations` is large, using checkpointing can help reduce shuffle file sizes on disk and help with failure recovery.

*Note*: LDA is a new feature with some missing functionality.  In particular, it does not yet
support prediction on new documents, and it does not have a Python API.  These will be added in the future.

**Examples**

In the following example, we load word count vectors representing a corpus of documents.
We then use [LDA](api/scala/index.html#org.apache.spark.mllib.clustering.LDA)
to infer three topics from the documents. The number of desired clusters is passed
to the algorithm. We then output the topics, represented as probability distributions over words.

<div class="codetabs">
<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("data/mllib/sample_lda_data.txt")
val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
// Index documents with unique IDs
val corpus = parsedData.zipWithIndex.map(_.swap).cache()

// Cluster the documents into three topics using LDA
val ldaModel = new LDA().setK(3).run(corpus)

// Output topics. Each is a distribution over words (matching word count vectors)
println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
val topics = ldaModel.topicsMatrix
for (topic <- Range(0, 3)) {
  print("Topic " + topic + ":")
  for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
  println()
}
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class JavaLDAExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("LDA Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load and parse the data
    String path = "data/mllib/sample_lda_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
        new Function<String, Vector>() {
          public Vector call(String s) {
            String[] sarray = s.trim().split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++)
              values[i] = Double.parseDouble(sarray[i]);
            return Vectors.dense(values);
          }
        }
    );
    // Index documents with unique IDs
    JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
        new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
          public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
            return doc_id.swap();
          }
        }
    ));
    corpus.cache();

    // Cluster the documents into three topics using LDA
    DistributedLDAModel ldaModel = new LDA().setK(3).run(corpus);

    // Output topics. Each is a distribution over words (matching word count vectors)
    System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
        + " words):");
    Matrix topics = ldaModel.topicsMatrix();
    for (int topic = 0; topic < 3; topic++) {
      System.out.print("Topic " + topic + ":");
      for (int word = 0; word < ldaModel.vocabSize(); word++) {
        System.out.print(" " + topics.apply(word, topic));
      }
      System.out.println();
    }
  }
}
{% endhighlight %}
</div>

</div>

## Streaming k-means

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

**Examples**

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
model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

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
