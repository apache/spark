---
layout: global
title: Ensembles
displayTitle: <a href="ml-guide.html">ML</a> - Ensembles
---

* Table of contents
{:toc}

An [ensemble method](http://en.wikipedia.org/wiki/Ensemble_learning)
is a learning algorithm which creates a model composed of a set of other base models.
ML supports the following ensemble algorithms: [`OneVsRest`](api/scala/index.html#org.apache.spark.ml.classifier.OneVsRest)

## OneVsRest

[OneVsRest](http://en.wikipedia.org/wiki/Multiclass_classification#One-vs.-rest) is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently.

`OneVsRest` is implemented as an `Estimator` takes as base classifier instances of `Classifier` and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.

Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.

### Example

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/), parse it as an RDD of `LabeledPoint` and perform multiclass classification using `OneVsRest`. The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

// parse data into dataframe
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_multiclass_classification_data.txt")
.toDF()
.withColumn("rnd", rand(0))

val train = data.filter($"rnd" < 0.8)
val test = data.filter($"rnd" >= 0.8)

// instantiate multiclass learner and train
val ovr = new OneVsRest().setClassifier(new LogisticRegression)

val ovrModel = ovr.fit(train)

// score model on test data
val predictions = ovrModel.transform(test).select("prediction", "label")

val predictionsRDD = predictions.map {case Row(p: Double, l: Double) => (p, l)}

// compute confusion matrix
val metrics = new MulticlassMetrics(predictionsRDD)

println(metrics.confusionMatrix)

// compute the false positive rate per label
val predictionColSchema = predictions.schema("prediction")
val numClasses = MetadataUtils.getNumClasses(predictionColSchema).get
val fprs = Range(0, numClasses).map(p => (p, metrics.falsePositiveRate(p.toDouble)))

println("label\tfpr")

println(fprs.map {case (label, fpr) => label + "\t" + fpr}.mkString("\n"))

{% endhighlight %}
</div>
