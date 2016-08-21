---
layout: global
title: Collaborative Filtering - RDD-based API
displayTitle: Collaborative Filtering - RDD-based API
---

* Table of contents
{:toc}

## Collaborative filtering 

[Collaborative filtering](http://en.wikipedia.org/wiki/Recommender_system#Collaborative_filtering)
is commonly used for recommender systems.  These techniques aim to fill in the
missing entries of a user-item association matrix.  `spark.mllib` currently supports
model-based collaborative filtering, in which users and products are described
by a small set of latent factors that can be used to predict missing entries.
`spark.mllib` uses the [alternating least squares
(ALS)](http://dl.acm.org/citation.cfm?id=1608614)
algorithm to learn these latent factors. The implementation in `spark.mllib` has the
following parameters:

* *numBlocks* is the number of blocks used to parallelize computation (set to -1 to auto-configure).
* *rank* is the number of latent factors in the model.
* *iterations* is the number of iterations of ALS to run. ALS typically converges to a reasonable
  solution in 20 iterations or less.
* *lambda* specifies the regularization parameter in ALS.
* *implicitPrefs* specifies whether to use the *explicit feedback* ALS variant or one adapted for
  *implicit feedback* data.
* *alpha* is a parameter applicable to the implicit feedback variant of ALS that governs the
  *baseline* confidence in preference observations.

### Explicit vs. implicit feedback

The standard approach to matrix factorization based collaborative filtering treats 
the entries in the user-item matrix as *explicit* preferences given by the user to the item,
for example, users giving ratings to movies.

It is common in many real-world use cases to only have access to *implicit feedback* (e.g. views,
clicks, purchases, likes, shares etc.). The approach used in `spark.mllib` to deal with such data is taken
from [Collaborative Filtering for Implicit Feedback Datasets](http://dx.doi.org/10.1109/ICDM.2008.22).
Essentially, instead of trying to model the matrix of ratings directly, this approach treats the data
as numbers representing the *strength* in observations of user actions (such as the number of clicks,
or the cumulative duration someone spent viewing a movie). Those numbers are then related to the level of
confidence in observed user preferences, rather than explicit ratings given to items. The model
then tries to find latent factors that can be used to predict the expected preference of a user for
an item.

### Scaling of the regularization parameter

Since v1.1, we scale the regularization parameter `lambda` in solving each least squares problem by
the number of ratings the user generated in updating user factors,
or the number of ratings the product received in updating product factors.
This approach is named "ALS-WR" and discussed in the paper
"[Large-Scale Parallel Collaborative Filtering for the Netflix Prize](http://dx.doi.org/10.1007/978-3-540-68880-8_32)".
It makes `lambda` less dependent on the scale of the dataset, so we can apply the
best parameter learned from a sampled subset to the full dataset and expect similar performance.

## Examples

<div class="codetabs">

<div data-lang="scala" markdown="1">
In the following example we load rating data. Each row consists of a user, a product and a rating.
We use the default [ALS.train()](api/scala/index.html#org.apache.spark.mllib.recommendation.ALS$) 
method which assumes ratings are explicit. We evaluate the
recommendation model by measuring the Mean Squared Error of rating prediction.

Refer to the [`ALS` Scala docs](api/scala/index.html#org.apache.spark.mllib.recommendation.ALS) for more details on the API.

{% include_example scala/org/apache/spark/examples/mllib/RecommendationExample.scala %}

If the rating matrix is derived from another source of information (i.e. it is inferred from
other signals), you can use the `trainImplicit` method to get better results.

{% highlight scala %}
val alpha = 0.01
val lambda = 0.01
val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object. A self-contained application example
that is equivalent to the provided example in Scala is given below:

Refer to the [`ALS` Java docs](api/java/org/apache/spark/mllib/recommendation/ALS.html) for more details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaRecommendationExample.java %}
</div>

<div data-lang="python" markdown="1">
In the following example we load rating data. Each row consists of a user, a product and a rating.
We use the default ALS.train() method which assumes ratings are explicit. We evaluate the
recommendation by measuring the Mean Squared Error of rating prediction.

Refer to the [`ALS` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.recommendation.ALS) for more details on the API.

{% include_example python/mllib/recommendation_example.py %}

If the rating matrix is derived from other source of information (i.e. it is inferred from other
signals), you can use the trainImplicit method to get better results.

{% highlight python %}
# Build the recommendation model using Alternating Least Squares based on implicit ratings
model = ALS.trainImplicit(ratings, rank, numIterations, alpha=0.01)
{% endhighlight %}
</div>

</div>

In order to run the above application, follow the instructions
provided in the [Self-Contained Applications](quick-start.html#self-contained-applications)
section of the Spark
Quick Start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.

## Tutorial

The [training exercises](https://databricks-training.s3.amazonaws.com/index.html) from the Spark Summit 2014 include a hands-on tutorial for
[personalized movie recommendation with `spark.mllib`](https://databricks-training.s3.amazonaws.com/movie-recommendation-with-mllib.html).
