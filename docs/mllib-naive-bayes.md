---
layout: global
title: <a href="mllib-guide.html">MLlib</a> - Naive Bayes
---

Naive Bayes is a simple multiclass classification algorithm with the assumption of independence
between every pair of features. Naive Bayes can be trained very efficiently. Within a single pass to
the training data, it computes the conditional probability distribution of each feature given label,
and then it applies Bayes' theorem to compute the conditional probability distribution of label
given an observation and use it for prediction. For more details, please visit the wikipedia page
[Naive Bayes classifier](http://en.wikipedia.org/wiki/Naive_Bayes_classifier).

In MLlib, we implemented multinomial naive Bayes, which is typically used for document
classification. Within that context, each observation is a document, each feature represents a term,
whose value is the frequency of the term. For its formulation, please visit the wikipedia page
[Multinomial naive Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier#Multinomial_naive_Bayes)
or the section
[Naive Bayes text classification](http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html)
from the book Introduction to Information
Retrieval. [Additive smoothing](http://en.wikipedia.org/wiki/Lidstone_smoothing) can be used by
setting the parameter $\lambda$ (default to $1.0$). For document classification, the input feature
vectors are usually sparse. Please supply sparse vectors as input to take advantage of
sparsity. Since the training data is only used once, it is not necessary to cache it.

## Examples

<div class="codetabs">
<div data-lang="scala" markdown="1">

[NaiveBayes](api/mllib/index.html#org.apache.spark.mllib.classification.NaiveBayes$) implements
multinomial naive Bayes. It takes an RDD of
[LabeledPoint](api/mllib/index.html#org.apache.spark.mllib.regression.LabeledPoint) and an optional
smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/mllib/index.html#org.apache.spark.mllib.classification.NaiveBayesModel), which
can be used for evaluation and prediction.

{% highlight scala %}
import org.apache.spark.mllib.classification.NaiveBayes

val training: RDD[LabeledPoint] = ... // training set
val test: RDD[LabeledPoint] = ... // test set

val model = NaiveBayes.train(training, lambda = 1.0)
val prediction = model.predict(test.map(_.features))

val predictionAndLabel = prediction.zip(test.map(_.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[NaiveBayes](api/mllib/index.html#org.apache.spark.mllib.classification.NaiveBayes$) implements
multinomial naive Bayes. It takes a Scala RDD of
[LabeledPoint](api/mllib/index.html#org.apache.spark.mllib.regression.LabeledPoint) and an
optionally smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/mllib/index.html#org.apache.spark.mllib.classification.NaiveBayesModel), which
can be used for evaluation and prediction.

{% highlight java %}
import org.apache.spark.mllib.classification.NaiveBayes;

JavaRDD<LabeledPoint> training = ... // training set
JavaRDD<LabeledPoint> test = ... // test set

NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

JavaRDD<Double> prediction = model.predict(test.map(new Function<LabeledPoint, Vector>() {
    public Vector call(LabeledPoint p) {
      return p.features();
    }
  })
JavaPairRDD<Double, Double> predictionAndLabel = 
  prediction.zip(test.map(new Function<LabeledPoint, Double>() {
    public Double call(LabeledPoint p) {
      return p.label();
    }
  })
double accuracy = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
    public Boolean call(Tuple2<Double, Double> pl) {
      return pl._1() == pl._2();
    }
  }).count() / test.count()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[NaiveBayes](api/pyspark/pyspark.mllib.classification.NaiveBayes-class.html) implements multinomial
naive Bayes. It takes an RDD of
[LabeledPoint](api/pyspark/pyspark.mllib.regression.LabeledPoint-class.html) and an optionally
smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/pyspark/pyspark.mllib.classification.NaiveBayesModel-class.html), which can be
used for evaluation and prediction.

<!--- TODO: Make Python's example consistent with Scala's and Java's. --->
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
