---
layout: global
title: MLlib - Collaborative Filtering 
---

* Table of contents
{:toc}

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


# Usage in Scala

Following code snippets can be executed in `spark-shell`.

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

# Usage in Java

All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object.

# Usage in Python
Following examples can be tested in the PySpark shell.

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
