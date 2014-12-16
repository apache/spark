---
layout: global
title: Naive Bayes - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Naive Bayes
---

[Naive Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier) is a simple
multiclass classification algorithm with the assumption of independence between
every pair of features. Naive Bayes can be trained very efficiently. Within a
single pass to the training data, it computes the conditional probability
distribution of each feature given label, and then it applies Bayes' theorem to
compute the conditional probability distribution of label given an observation
and use it for prediction.

MLlib supports [multinomial naive
Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier#Multinomial_naive_Bayes),
which is typically used for [document
classification](http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html).
Within that context, each observation is a document and each
feature represents a term whose value is the frequency of the term.
Feature values must be nonnegative to represent term frequencies.
[Additive smoothing](http://en.wikipedia.org/wiki/Lidstone_smoothing) can be used by
setting the parameter $\lambda$ (default to $1.0$). For document classification, the input feature
vectors are usually sparse, and sparse vectors should be supplied as input to take advantage of
sparsity. Since the training data is only used once, it is not necessary to cache it.

## Examples

<div class="codetabs">
<div data-lang="scala" markdown="1">

[NaiveBayes](api/scala/index.html#org.apache.spark.mllib.classification.NaiveBayes$) implements
multinomial naive Bayes. It takes an RDD of
[LabeledPoint](api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint) and an optional
smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/scala/index.html#org.apache.spark.mllib.classification.NaiveBayesModel), which
can be used for evaluation and prediction.

{% highlight scala %}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

val data = sc.textFile("data/mllib/sample_naive_bayes_data.txt")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}
// Split data into training (60%) and test (40%).
val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0)
val test = splits(1)

val model = NaiveBayes.train(training, lambda = 1.0)

val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[NaiveBayes](api/java/org/apache/spark/mllib/classification/NaiveBayes.html) implements
multinomial naive Bayes. It takes a Scala RDD of
[LabeledPoint](api/java/org/apache/spark/mllib/regression/LabeledPoint.html) and an
optionally smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/java/org/apache/spark/mllib/classification/NaiveBayesModel.html), which
can be used for evaluation and prediction.

{% highlight java %}
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

JavaRDD<LabeledPoint> training = ... // training set
JavaRDD<LabeledPoint> test = ... // test set

final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

JavaPairRDD<Double, Double> predictionAndLabel = 
  test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
    @Override public Boolean call(Tuple2<Double, Double> pl) {
      return pl._1().equals(pl._2());
    }
  }).count() / (double) test.count();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[NaiveBayes](api/python/pyspark.mllib.classification.NaiveBayes-class.html) implements multinomial
naive Bayes. It takes an RDD of
[LabeledPoint](api/python/pyspark.mllib.regression.LabeledPoint-class.html) and an optionally
smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/python/pyspark.mllib.classification.NaiveBayesModel-class.html), which can be
used for evaluation and prediction.

<!-- TODO: Make Python's example consistent with Scala's and Java's. -->
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

# an RDD of LabeledPoint
data = sc.parallelize([
  LabeledPoint(0.0, [0.0, 0.0])
  ... # more labeled points
])

# Train a naive Bayes model.
model = NaiveBayes.train(data, 1.0)

# Make prediction.
prediction = model.predict([0.0, 0.0])
{% endhighlight %}

</div>
</div>
