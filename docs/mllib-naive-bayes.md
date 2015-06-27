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
Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier#Multinomial_naive_Bayes)
and [Bernoulli naive Bayes](http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html).
These models are typically used for [document classification](http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html).
Within that context, each observation is a document and each
feature represents a term whose value is the frequency of the term (in multinomial naive Bayes) or
a zero or one indicating whether the term was found in the document (in Bernoulli naive Bayes).
Feature values must be nonnegative. The model type is selected with an optional parameter
"multinomial" or "bernoulli" with "multinomial" as the default.
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
smoothing parameter `lambda` as input, an optional model type parameter (default is "multinomial"), and outputs a
[NaiveBayesModel](api/scala/index.html#org.apache.spark.mllib.classification.NaiveBayesModel), which
can be used for evaluation and prediction.

{% highlight scala %}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
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

val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

// Save and load model
model.save(sc, "myModelPath")
val sameModel = NaiveBayesModel.load(sc, "myModelPath")
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
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

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

// Save and load model
model.save(sc.sc(), "myModelPath");
NaiveBayesModel sameModel = NaiveBayesModel.load(sc.sc(), "myModelPath");
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[NaiveBayes](api/python/pyspark.mllib.html#pyspark.mllib.classification.NaiveBayes) implements multinomial
naive Bayes. It takes an RDD of
[LabeledPoint](api/python/pyspark.mllib.html#pyspark.mllib.regression.LabeledPoint) and an optionally
smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/python/pyspark.mllib.html#pyspark.mllib.classification.NaiveBayesModel), which can be
used for evaluation and prediction.

Note that the Python API does not yet support model save/load but will in the future.

{% highlight python %}
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

def parseLine(line):
    parts = line.split(',')
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(' ')])
    return LabeledPoint(label, features)

data = sc.textFile('data/mllib/sample_naive_bayes_data.txt').map(parseLine)

# Split data aproximately into training (60%) and test (40%)
training, test = data.randomSplit([0.6, 0.4], seed = 0)

# Train a naive Bayes model.
model = NaiveBayes.train(training, 1.0)

# Make prediction and test accuracy.
predictionAndLabel = test.map(lambda p : (model.predict(p.features), p.label))
accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
{% endhighlight %}

</div>
</div>
