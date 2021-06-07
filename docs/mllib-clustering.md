---
layout: global
title: Clustering - RDD-based API
displayTitle: Clustering - RDD-based API
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

[Clustering](https://en.wikipedia.org/wiki/Cluster_analysis) is an unsupervised learning problem whereby we aim to group subsets
of entities with one another based on some notion of similarity.  Clustering is
often used for exploratory analysis and/or as a component of a hierarchical
[supervised learning](https://en.wikipedia.org/wiki/Supervised_learning) pipeline (in which distinct classifiers or regression
models are trained for each cluster).

The `spark.mllib` package supports the following models:

* Table of contents
{:toc}

## K-means

[K-means](http://en.wikipedia.org/wiki/K-means_clustering) is one of the
most commonly used clustering algorithms that clusters the data points into a
predefined number of clusters. The `spark.mllib` implementation includes a parallelized
variant of the [k-means++](http://en.wikipedia.org/wiki/K-means%2B%2B) method
called [kmeans||](http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf).
The implementation in `spark.mllib` has the following parameters:

* *k* is the number of desired clusters. Note that it is possible for fewer than k clusters to be returned, for example, if there are fewer than k distinct points to cluster.
* *maxIterations* is the maximum number of iterations to run.
* *initializationMode* specifies either random initialization or
initialization via k-means\|\|.
* *runs* This param has no effect since Spark 2.0.0.
* *initializationSteps* determines the number of steps in the k-means\|\| algorithm.
* *epsilon* determines the distance threshold within which we consider k-means to have converged.
* *initialModel* is an optional set of cluster centers used for initialization. If this parameter is supplied, only one run is performed.

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">
The following code snippets can be executed in `spark-shell`.

In the following example after loading and parsing data, we use the
[`KMeans`](api/scala/org/apache/spark/mllib/clustering/KMeans.html) object to cluster the data
into two clusters. The number of desired clusters is passed to the algorithm. We then compute Within
Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In fact, the
optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

Refer to the [`KMeans` Scala docs](api/scala/org/apache/spark/mllib/clustering/KMeans.html) and [`KMeansModel` Scala docs](api/scala/org/apache/spark/mllib/clustering/KMeansModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/KMeansExample.scala %}
</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. A self-contained application example
that is equivalent to the provided example in Scala is given below:

Refer to the [`KMeans` Java docs](api/java/org/apache/spark/mllib/clustering/KMeans.html) and [`KMeansModel` Java docs](api/java/org/apache/spark/mllib/clustering/KMeansModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaKMeansExample.java %}
</div>

<div data-lang="python" markdown="1">
The following examples can be tested in the PySpark shell.

In the following example after loading and parsing data, we use the KMeans object to cluster the
data into two clusters. The number of desired clusters is passed to the algorithm. We then compute
Within Set Sum of Squared Error (WSSSE). You can reduce this error measure by increasing *k*. In
fact the optimal *k* is usually one where there is an "elbow" in the WSSSE graph.

Refer to the [`KMeans` Python docs](api/python/reference/api/pyspark.mllib.clustering.KMeans.html) and [`KMeansModel` Python docs](api/python/reference/api/pyspark.mllib.clustering.KMeansModel.html) for more details on the API.

{% include_example python/mllib/k_means_example.py %}
</div>

</div>

## Gaussian mixture

A [Gaussian Mixture Model](http://en.wikipedia.org/wiki/Mixture_model#Multivariate_Gaussian_mixture_model)
represents a composite distribution whereby points are drawn from one of *k* Gaussian sub-distributions,
each with its own probability.  The `spark.mllib` implementation uses the
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
[GaussianMixture](api/scala/org/apache/spark/mllib/clustering/GaussianMixture.html)
object to cluster the data into two clusters. The number of desired clusters is passed
to the algorithm. We then output the parameters of the mixture model.

Refer to the [`GaussianMixture` Scala docs](api/scala/org/apache/spark/mllib/clustering/GaussianMixture.html) and [`GaussianMixtureModel` Scala docs](api/scala/org/apache/spark/mllib/clustering/GaussianMixtureModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/GaussianMixtureExample.scala %}
</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. A self-contained application example
that is equivalent to the provided example in Scala is given below:

Refer to the [`GaussianMixture` Java docs](api/java/org/apache/spark/mllib/clustering/GaussianMixture.html) and [`GaussianMixtureModel` Java docs](api/java/org/apache/spark/mllib/clustering/GaussianMixtureModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaGaussianMixtureExample.java %}
</div>

<div data-lang="python" markdown="1">
In the following example after loading and parsing data, we use a
[GaussianMixture](api/python/reference/api/pyspark.mllib.clustering.GaussianMixture.html)
object to cluster the data into two clusters. The number of desired clusters is passed
to the algorithm. We then output the parameters of the mixture model.

Refer to the [`GaussianMixture` Python docs](api/python/reference/api/pyspark.mllib.clustering.GaussianMixture.html) and [`GaussianMixtureModel` Python docs](api/python/reference/api/pyspark.mllib.clustering.GaussianMixtureModel.html) for more details on the API.

{% include_example python/mllib/gaussian_mixture_example.py %}
</div>

</div>

## Power iteration clustering (PIC)

Power iteration clustering (PIC) is a scalable and efficient algorithm for clustering vertices of a
graph given pairwise similarities as edge properties,
described in [Lin and Cohen, Power Iteration Clustering](http://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf).
It computes a pseudo-eigenvector of the normalized affinity matrix of the graph via
[power iteration](http://en.wikipedia.org/wiki/Power_iteration)  and uses it to cluster vertices.
`spark.mllib` includes an implementation of PIC using GraphX as its backend.
It takes an `RDD` of `(srcId, dstId, similarity)` tuples and outputs a model with the clustering assignments.
The similarities must be nonnegative.
PIC assumes that the similarity measure is symmetric.
A pair `(srcId, dstId)` regardless of the ordering should appear at most once in the input data.
If a pair is missing from input, their similarity is treated as zero.
`spark.mllib`'s PIC implementation takes the following (hyper-)parameters:

* `k`: number of clusters
* `maxIterations`: maximum number of power iterations
* `initializationMode`: initialization model. This can be either "random", which is the default,
  to use a random vector as vertex properties, or "degree" to use normalized sum similarities.

**Examples**

In the following, we show code snippets to demonstrate how to use PIC in `spark.mllib`.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`PowerIterationClustering`](api/scala/org/apache/spark/mllib/clustering/PowerIterationClustering.html) 
implements the PIC algorithm.
It takes an `RDD` of `(srcId: Long, dstId: Long, similarity: Double)` tuples representing the
affinity matrix.
Calling `PowerIterationClustering.run` returns a
[`PowerIterationClusteringModel`](api/scala/org/apache/spark/mllib/clustering/PowerIterationClusteringModel.html),
which contains the computed clustering assignments.

Refer to the [`PowerIterationClustering` Scala docs](api/scala/org/apache/spark/mllib/clustering/PowerIterationClustering.html) and [`PowerIterationClusteringModel` Scala docs](api/scala/org/apache/spark/mllib/clustering/PowerIterationClusteringModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/PowerIterationClusteringExample.scala %}
</div>

<div data-lang="java" markdown="1">

[`PowerIterationClustering`](api/java/org/apache/spark/mllib/clustering/PowerIterationClustering.html)
implements the PIC algorithm.
It takes a `JavaRDD` of `(srcId: Long, dstId: Long, similarity: Double)` tuples representing the
affinity matrix.
Calling `PowerIterationClustering.run` returns a
[`PowerIterationClusteringModel`](api/java/org/apache/spark/mllib/clustering/PowerIterationClusteringModel.html)
which contains the computed clustering assignments.

Refer to the [`PowerIterationClustering` Java docs](api/java/org/apache/spark/mllib/clustering/PowerIterationClustering.html) and [`PowerIterationClusteringModel` Java docs](api/java/org/apache/spark/mllib/clustering/PowerIterationClusteringModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaPowerIterationClusteringExample.java %}
</div>

<div data-lang="python" markdown="1">

[`PowerIterationClustering`](api/python/reference/api/pyspark.mllib.clustering.PowerIterationClustering.html)
implements the PIC algorithm.
It takes an `RDD` of `(srcId: Long, dstId: Long, similarity: Double)` tuples representing the
affinity matrix.
Calling `PowerIterationClustering.run` returns a
[`PowerIterationClusteringModel`](api/python/reference/api/pyspark.mllib.clustering.PowerIterationClustering.html),
which contains the computed clustering assignments.

Refer to the [`PowerIterationClustering` Python docs](api/python/reference/api/pyspark.mllib.clustering.PowerIterationClustering.html) and [`PowerIterationClusteringModel` Python docs](api/python/reference/api/pyspark.mllib.clustering.PowerIterationClusteringModel.html) for more details on the API.

{% include_example python/mllib/power_iteration_clustering_example.py %}
</div>

</div>

## Latent Dirichlet allocation (LDA)

[Latent Dirichlet allocation (LDA)](http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)
is a topic model which infers topics from a collection of text documents.
LDA can be thought of as a clustering algorithm as follows:

* Topics correspond to cluster centers, and documents correspond to
examples (rows) in a dataset.
* Topics and documents both exist in a feature space, where feature
vectors are vectors of word counts (bag of words).
* Rather than estimating a clustering using a traditional distance, LDA
uses a function based on a statistical model of how text documents are
generated.

LDA supports different inference algorithms via `setOptimizer` function.
`EMLDAOptimizer` learns clustering using
[expectation-maximization](http://en.wikipedia.org/wiki/Expectation%E2%80%93maximization_algorithm)
on the likelihood function and yields comprehensive results, while
`OnlineLDAOptimizer` uses iterative mini-batch sampling for [online
variational
inference](https://mimno.infosci.cornell.edu/info6150/readings/HoffmanBleiBach2010b.pdf)
and is generally memory friendly.

LDA takes in a collection of documents as vectors of word counts and the
following parameters (set using the builder pattern):

* `k`: Number of topics (i.e., cluster centers)
* `optimizer`: Optimizer to use for learning the LDA model, either
`EMLDAOptimizer` or `OnlineLDAOptimizer`
* `docConcentration`: Dirichlet parameter for prior over documents'
distributions over topics. Larger values encourage smoother inferred
distributions.
* `topicConcentration`: Dirichlet parameter for prior over topics'
distributions over terms (words). Larger values encourage smoother
inferred distributions.
* `maxIterations`: Limit on the number of iterations.
* `checkpointInterval`: If using checkpointing (set in the Spark
configuration), this parameter specifies the frequency with which
checkpoints will be created.  If `maxIterations` is large, using
checkpointing can help reduce shuffle file sizes on disk and help with
failure recovery.


All of `spark.mllib`'s LDA models support:

* `describeTopics`: Returns topics as arrays of most important terms and
term weights
* `topicsMatrix`: Returns a `vocabSize` by `k` matrix where each column
is a topic

*Note*: LDA is still an experimental feature under active development.
As a result, certain features are only available in one of the two
optimizers / models generated by the optimizer. Currently, a distributed
model can be converted into a local model, but not vice-versa.

The following discussion will describe each optimizer/model pair
separately.

**Expectation Maximization**

Implemented in
[`EMLDAOptimizer`](api/scala/org/apache/spark/mllib/clustering/EMLDAOptimizer.html)
and
[`DistributedLDAModel`](api/scala/org/apache/spark/mllib/clustering/DistributedLDAModel.html).

For the parameters provided to `LDA`:

* `docConcentration`: Only symmetric priors are supported, so all values
in the provided `k`-dimensional vector must be identical. All values
must also be $> 1.0$. Providing `Vector(-1)` results in default behavior
(uniform `k` dimensional vector with value $(50 / k) + 1$
* `topicConcentration`: Only symmetric priors supported. Values must be
$> 1.0$. Providing `-1` results in defaulting to a value of $0.1 + 1$.
* `maxIterations`: The maximum number of EM iterations.

*Note*: It is important to do enough iterations.  In early iterations, EM often has useless topics,
but those topics improve dramatically after more iterations.  Using at least 20 and possibly
50-100 iterations is often reasonable, depending on your dataset.

`EMLDAOptimizer` produces a `DistributedLDAModel`, which stores not only
the inferred topics but also the full training corpus and topic
distributions for each document in the training corpus. A
`DistributedLDAModel` supports:

 * `topTopicsPerDocument`: The top topics and their weights for
 each document in the training corpus
 * `topDocumentsPerTopic`: The top documents for each topic and
 the corresponding weight of the topic in the documents.
 * `logPrior`: log probability of the estimated topics and
 document-topic distributions given the hyperparameters
 `docConcentration` and `topicConcentration`
 * `logLikelihood`: log likelihood of the training corpus, given the
 inferred topics and document-topic distributions

**Online Variational Bayes**

Implemented in
[`OnlineLDAOptimizer`](api/scala/org/apache/spark/mllib/clustering/OnlineLDAOptimizer.html)
and
[`LocalLDAModel`](api/scala/org/apache/spark/mllib/clustering/LocalLDAModel.html).

For the parameters provided to `LDA`:

* `docConcentration`: Asymmetric priors can be used by passing in a
vector with values equal to the Dirichlet parameter in each of the `k`
dimensions. Values should be $>= 0$. Providing `Vector(-1)` results in
default behavior (uniform `k` dimensional vector with value $(1.0 / k)$)
* `topicConcentration`: Only symmetric priors supported. Values must be
$>= 0$. Providing `-1` results in defaulting to a value of $(1.0 / k)$.
* `maxIterations`: Maximum number of minibatches to submit.

In addition, `OnlineLDAOptimizer` accepts the following parameters:

* `miniBatchFraction`: Fraction of corpus sampled and used at each
iteration
* `optimizeDocConcentration`: If set to true, performs maximum-likelihood
estimation of the hyperparameter `docConcentration` (aka `alpha`)
after each minibatch and sets the optimized `docConcentration` in the
returned `LocalLDAModel`
* `tau0` and `kappa`: Used for learning-rate decay, which is computed by
$(\tau_0 + iter)^{-\kappa}$ where $iter$ is the current number of iterations.

`OnlineLDAOptimizer` produces a `LocalLDAModel`, which only stores the
inferred topics. A `LocalLDAModel` supports:

* `logLikelihood(documents)`: Calculates a lower bound on the provided
`documents` given the inferred topics.
* `logPerplexity(documents)`: Calculates an upper bound on the
perplexity of the provided `documents` given the inferred topics.

**Examples**

In the following example, we load word count vectors representing a corpus of documents.
We then use [LDA](api/scala/org/apache/spark/mllib/clustering/LDA.html)
to infer three topics from the documents. The number of desired clusters is passed
to the algorithm. We then output the topics, represented as probability distributions over words.

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`LDA` Scala docs](api/scala/org/apache/spark/mllib/clustering/LDA.html) and [`DistributedLDAModel` Scala docs](api/scala/org/apache/spark/mllib/clustering/DistributedLDAModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/LatentDirichletAllocationExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`LDA` Java docs](api/java/org/apache/spark/mllib/clustering/LDA.html) and [`DistributedLDAModel` Java docs](api/java/org/apache/spark/mllib/clustering/DistributedLDAModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaLatentDirichletAllocationExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`LDA` Python docs](api/python/reference/api/pyspark.mllib.clustering.LDA.html) and [`LDAModel` Python docs](api/python/reference/api/pyspark.mllib.clustering.LDAModel.html) for more details on the API.

{% include_example python/mllib/latent_dirichlet_allocation_example.py %}
</div>

</div>

## Bisecting k-means

Bisecting K-means can often be much faster than regular K-means, but it will generally produce a different clustering.

Bisecting k-means is a kind of [hierarchical clustering](https://en.wikipedia.org/wiki/Hierarchical_clustering).
Hierarchical clustering is one of the most commonly used  method of cluster analysis which seeks to build a hierarchy of clusters.
Strategies for hierarchical clustering generally fall into two types:

- Agglomerative: This is a "bottom up" approach: each observation starts in its own cluster, and pairs of clusters are merged as one moves up the hierarchy.
- Divisive: This is a "top down" approach: all observations start in one cluster, and splits are performed recursively as one moves down the hierarchy.

Bisecting k-means algorithm is a kind of divisive algorithms.
The implementation in MLlib has the following parameters:

* *k*: the desired number of leaf clusters (default: 4). The actual number could be smaller if there are no divisible leaf clusters.
* *maxIterations*: the max number of k-means iterations to split clusters (default: 20)
* *minDivisibleClusterSize*: the minimum number of points (if >= 1.0) or the minimum proportion of points (if < 1.0) of a divisible cluster (default: 1)
* *seed*: a random seed (default: hash value of the class name)

**Examples**

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`BisectingKMeans` Scala docs](api/scala/org/apache/spark/mllib/clustering/BisectingKMeans.html) and [`BisectingKMeansModel` Scala docs](api/scala/org/apache/spark/mllib/clustering/BisectingKMeansModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/BisectingKMeansExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`BisectingKMeans` Java docs](api/java/org/apache/spark/mllib/clustering/BisectingKMeans.html) and [`BisectingKMeansModel` Java docs](api/java/org/apache/spark/mllib/clustering/BisectingKMeansModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaBisectingKMeansExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`BisectingKMeans` Python docs](api/python/reference/api/pyspark.mllib.clustering.BisectingKMeans.html) and [`BisectingKMeansModel` Python docs](api/python/reference/api/pyspark.mllib.clustering.BisectingKMeansModel.html) for more details on the API.

{% include_example python/mllib/bisecting_k_means_example.py %}
</div>
</div>

## Streaming k-means

When data arrive in a stream, we may want to estimate clusters dynamically,
updating them as new data arrive. `spark.mllib` provides support for streaming k-means clustering,
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
Refer to the [`StreamingKMeans` Scala docs](api/scala/org/apache/spark/mllib/clustering/StreamingKMeans.html) for details on the API.
And Refer to [Spark Streaming Programming Guide](streaming-programming-guide.html#initializing) for details on StreamingContext.

{% include_example scala/org/apache/spark/examples/mllib/StreamingKMeansExample.scala %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`StreamingKMeans` Python docs](api/python/reference/api/pyspark.mllib.clustering.StreamingKMeans.html) for more details on the API.
And Refer to [Spark Streaming Programming Guide](streaming-programming-guide.html#initializing) for details on StreamingContext.

{% include_example python/mllib/streaming_k_means_example.py %}
</div>

</div>

As you add new text files with data the cluster centers will update. Each training
point should be formatted as `[x1, x2, x3]`, and each test data point
should be formatted as `(y, [x1, x2, x3])`, where `y` is some useful label or identifier
(e.g. a true category assignment). Anytime a text file is placed in `/training/data/dir`
the model will update. Anytime a text file is placed in `/testing/data/dir`
you will see predictions. With new data, the cluster centers will change!
