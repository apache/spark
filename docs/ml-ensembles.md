---
layout: global
title: Ensembles
displayTitle: <a href="ml-guide.html">ML</a> - Ensembles
---

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

An [ensemble method](http://en.wikipedia.org/wiki/Ensemble_learning)
is a learning algorithm which creates a model composed of a set of other base models.
The Pipelines API supports the following ensemble algorithms: [`OneVsRest`](api/scala/index.html#org.apache.spark.ml.classifier.OneVsRest)

## OneVsRest

[OneVsRest](http://en.wikipedia.org/wiki/Multiclass_classification#One-vs.-rest) is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently.

`OneVsRest` is implemented as an `Estimator`. For the base classifier it takes instances of `Classifier` and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.

Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.

### Example

The example below demonstrates how to load the
[Iris dataset](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/iris.scale), parse it as a DataFrame and perform multiclass classification using `OneVsRest`. The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}

val sqlContext = new SQLContext(sc)

// parse data into dataframe
val data = MLUtils.loadLibSVMFile(sc, 
  "data/mllib/sample_multiclass_classification_data.txt")
val Array(train, test) = data.toDF().randomSplit(Array(0.7, 0.3))

// instantiate multiclass learner and train
val ovr = new OneVsRest().setClassifier(new LogisticRegression)

val ovrModel = ovr.fit(train)

// score model on test data
val predictions = ovrModel.transform(test).select("prediction", "label")
val predictionsAndLabels = predictions.map {case Row(p: Double, l: Double) => (p, l)}

// compute confusion matrix
val metrics = new MulticlassMetrics(predictionsAndLabels)
println(metrics.confusionMatrix)

// the Iris DataSet has three classes
val numClasses = 3

println("label\tfpr\n")
(0 until numClasses).foreach { index =>
  val label = index.toDouble
  println(label + "\t" + metrics.falsePositiveRate(label))
}
{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

SparkConf conf = new SparkConf().setAppName("JavaOneVsRestExample");
JavaSparkContext jsc = new JavaSparkContext(conf);
SQLContext jsql = new SQLContext(jsc);

RDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(),
  "data/mllib/sample_multiclass_classification_data.txt");

DataFrame dataFrame = jsql.createDataFrame(data, LabeledPoint.class);
DataFrame[] splits = dataFrame.randomSplit(new double[]{0.7, 0.3}, 12345);
DataFrame train = splits[0];
DataFrame test = splits[1];

// instantiate the One Vs Rest Classifier
OneVsRest ovr = new OneVsRest().setClassifier(new LogisticRegression());

// train the multiclass model
OneVsRestModel ovrModel = ovr.fit(train.cache());

// score the model on test data
DataFrame predictions = ovrModel
  .transform(test)
  .select("prediction", "label");

// obtain metrics
MulticlassMetrics metrics = new MulticlassMetrics(predictions);
Matrix confusionMatrix = metrics.confusionMatrix();

// output the Confusion Matrix
System.out.println("Confusion Matrix");
System.out.println(confusionMatrix);

// compute the false positive rate per label
System.out.println();
System.out.println("label\tfpr\n");

// the Iris DataSet has three classes
int numClasses = 3;
for (int index = 0; index < numClasses; index++) {
  double label = (double) index;
  System.out.print(label);
  System.out.print("\t");
  System.out.print(metrics.falsePositiveRate(label));
  System.out.println();
}
{% endhighlight %}
</div>
</div>
