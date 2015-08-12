---
layout: global
title: Evaluation Metrics - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Evaluation Metrics
---

* Table of contents
{:toc}

Spark's MLlib comes with a number of machine learning algorithms that can be used to learn from and make predictions
on data. When these algorithms are applied to build machine learning models, there is a need to evaluate the performance
of the model on some criteria, which depends on the application and its requirements. Spark's MLlib also provides a
suite of metrics for the purpose of evaluating the performance of machine learning models.

Specific machine learning algorithms fall under broader types of machine learning applications like classification,
regression, clustering, etc. Each of these types have well established metrics for performance evaluation and those
metrics that are currently available in Spark's MLlib are detailed in this section.

## Classification model evaluation

While there are many different types of classification algorithms, the evaluation of classification models all share
similar principles. In a [supervised classification problem](https://en.wikipedia.org/wiki/Statistical_classification),
there exists a true output and a model-generated predicted output for each data point. For this reason, the results for
each data point can be assigned to one of four categories:

* True Positive (TP) - label is positive and prediction is also positive
* True Negative (TN) - label is negative and prediction is also negative
* False Positive (FP) - label is negative but prediction is positive
* False Negative (FN) - label is positive but prediction is negative

These four numbers are the building blocks for most classifier evaluation metrics. A fundamental point when considering
classifier evaluation is that pure accuracy (i.e. was the prediction correct or incorrect) is not generally a good metric. The
reason for this is because a dataset may be highly unbalanced. For example, if a model is designed to predict fraud from
a dataset where 95% of the data points are _not fraud_ and 5% of the data points are _fraud_, then a naive classifier
that predicts _not fraud_, regardless of input, will be 95% accurate. For this reason, metrics like
[precision and recall](https://en.wikipedia.org/wiki/Precision_and_recall) are typically used because they take into
account the *type* of error. In most applications there is some desired balance between precision and recall, which can
be captured by combining the two into a single metric, called the [F-measure](https://en.wikipedia.org/wiki/F1_score).

### Binary classification

[Binary classifiers](https://en.wikipedia.org/wiki/Binary_classification) are used to separate the elements of a given
dataset into one of two possible groups (e.g. fraud or not fraud) and is a special case of multiclass classification.
Most binary classification metrics can be generalized to multiclass classification metrics.

#### Threshold tuning

It is import to understand that many classification models actually output a "score" (often times a probability) for
each class, where a higher score indicates higher likelihood. In the binary case, the model may output a probability for
each class: $P(Y=1|X)$ and $P(Y=0|X)$. Instead of simply taking the higher probability, there may be some cases where
the model might need to be tuned so that it only predicts a class when the probability is very high (e.g. only block a
credit card transaction if the model predicts fraud with >90% probability). Therefore, there is a prediction *threshold*
which determines what the predicted class will be based on the probabilities that the model outputs.

Tuning the prediction threshold will change the precision and recall of the model and is an important part of model
optimization. In order to visualize how precision, recall, and other metrics change as a function of the threshold it is
common practice to plot competing metrics against one another, parameterized by threshold. A P-R curve plots (precision,
recall) points for different threshold values, while a
[receiver operating characteristic](https://en.wikipedia.org/wiki/Receiver_operating_characteristic), or ROC, curve
plots (recall, false positive rate) points.

**Available metrics**

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Precision (Postive Predictive Value)</td>
      <td>$PPV=\frac{TP}{TP + FP}$</td>
    </tr>
    <tr>
      <td>Recall (True Positive Rate)</td>
      <td>$TPR=\frac{TP}{P}=\frac{TP}{TP + FN}$</td>
    </tr>
    <tr>
      <td>F-measure</td>
      <td>$F(\beta) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV \cdot TPR}
          {\beta^2 \cdot PPV + TPR}\right)$</td>
    </tr>
    <tr>
      <td>Receiver Operating Characteristic (ROC)</td>
      <td>$FPR(T)=\int^\infty_{T} P_0(T)\,dT \\ TPR(T)=\int^\infty_{T} P_1(T)\,dT$</td>
    </tr>
    <tr>
      <td>Area Under ROC Curve</td>
      <td>$AUROC=\int^1_{0} \frac{TP}{P} d\left(\frac{FP}{N}\right)$</td>
    </tr>
    <tr>
      <td>Area Under Precision-Recall Curve</td>
      <td>$AUPRC=\int^1_{0} \frac{TP}{TP+FP} d\left(\frac{TP}{P}\right)$</td>
    </tr>
  </tbody>
</table>


**Examples**

<div class="codetabs">
The following code snippets illustrate how to load a sample dataset, train a binary classification algorithm on the
data, and evaluate the performance of the algorithm by several binary evaluation metrics.

<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_binary_classification_data.txt")

// Split data into training (60%) and test (40%)
val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
training.cache()

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(2)
  .run(training)

// Clear the prediction threshold so the model will return probabilities
model.clearThreshold

// Compute raw scores on the test set
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object
val metrics = new BinaryClassificationMetrics(predictionAndLabels)

// Precision by threshold
val precision = metrics.precisionByThreshold
precision.foreach { case (t, p) =>
    println(s"Threshold: $t, Precision: $p")
}

// Recall by threshold
val recall = metrics.precisionByThreshold
recall.foreach { case (t, r) =>
    println(s"Threshold: $t, Recall: $r")
}

// Precision-Recall Curve
val PRC = metrics.pr

// F-measure
val f1Score = metrics.fMeasureByThreshold
f1Score.foreach { case (t, f) =>
    println(s"Threshold: $t, F-score: $f, Beta = 1")
}

val beta = 0.5
val fScore = metrics.fMeasureByThreshold(beta)
f1Score.foreach { case (t, f) =>
    println(s"Threshold: $t, F-score: $f, Beta = 0.5")
}

// AUPRC
val auPRC = metrics.areaUnderPR
println("Area under precision-recall curve = " + auPRC)

// Compute thresholds used in ROC and PR curves
val thresholds = precision.map(_._1)

// ROC Curve
val roc = metrics.roc

// AUROC
val auROC = metrics.areaUnderROC
println("Area under ROC = " + auROC)

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class BinaryClassification {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Binary Classification Metrics");
    SparkContext sc = new SparkContext(conf);
    String path = "data/mllib/sample_binary_classification_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    JavaRDD<LabeledPoint> test = splits[1];

    // Run training algorithm to build the model.
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training.rdd());

    // Clear the prediction threshold so the model will return probabilities
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double prediction = model.predict(p.features());
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      }
    );

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());

    // Precision by threshold
    JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
    System.out.println("Precision by threshold: " + precision.toArray());

    // Recall by threshold
    JavaRDD<Tuple2<Object, Object>> recall = metrics.recallByThreshold().toJavaRDD();
    System.out.println("Recall by threshold: " + recall.toArray());

    // F Score by threshold
    JavaRDD<Tuple2<Object, Object>> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
    System.out.println("F1 Score by threshold: " + f1Score.toArray());

    JavaRDD<Tuple2<Object, Object>> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
    System.out.println("F2 Score by threshold: " + f2Score.toArray());

    // Precision-recall curve
    JavaRDD<Tuple2<Object, Object>> prc = metrics.pr().toJavaRDD();
    System.out.println("Precision-recall curve: " + prc.toArray());

    // Thresholds
    JavaRDD<Double> thresholds = precision.map(
      new Function<Tuple2<Object, Object>, Double>() {
        public Double call (Tuple2<Object, Object> t) {
          return new Double(t._1().toString());
        }
      }
    );

    // ROC Curve
    JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
    System.out.println("ROC curve: " + roc.toArray());

    // AUPRC
    System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

    // AUROC
    System.out.println("Area under ROC = " + metrics.areaUnderROC());

    // Save and load model
    model.save(sc, "myModelPath");
    LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc, "myModelPath");
  }
}

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

# Several of the methods available in scala are currently missing from pyspark

# Load training data in LIBSVM format
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_binary_classification_data.txt")

# Split data into training (60%) and test (40%)
training, test = data.randomSplit([0.6, 0.4], seed = 11L)
training.cache()

# Run training algorithm to build the model
model = LogisticRegressionWithLBFGS.train(training)

# Compute raw scores on the test set
predictionAndLabels = test.map(lambda lp: (float(model.predict(lp.features)), lp.label))

# Instantiate metrics object
metrics = BinaryClassificationMetrics(predictionAndLabels)

# Area under precision-recall curve
print("Area under PR = %s" % metrics.areaUnderPR)

# Area under ROC curve
print("Area under ROC = %s" % metrics.areaUnderROC)

{% endhighlight %}

</div>
</div>


### Multiclass classification

A [multiclass classification](https://en.wikipedia.org/wiki/Multiclass_classification) describes a classification
problem where there are $M \gt 2$ possible labels for each data point (the case where $M=2$ is the binary
classification problem). For example, classifying handwriting samples to the digits 0 to 9, having 10 possible classes.

For multiclass metrics, the notion of positives and negatives is slightly different. Predictions and labels can still
be positive or negative, but they must be considered under the context of a particular class. Each label and prediction
take on the value of one of the multiple classes and so they are said to be positive for their particular class and negative
for all other classes. So, a true positive occurs whenever the prediction and the label match, while a true negative
occurs when neither the prediction nor the label take on the value of a given class. By this convention, there can be
multiple true negatives for a given data sample. The extension of false negatives and false positives from the former
definitions of positive and negative labels is straightforward.

#### Label based metrics

Opposed to binary classification where there are only two possible labels, multiclass classification problems have many
possible labels and so the concept of label-based metrics is introduced. Overall precision measures precision across all
labels -  the number of times any class was predicted correctly (true positives) normalized by the number of data
points. Precision by label considers only one class, and measures the number of time a specific label was predicted
correctly normalized by the number of times that label appears in the output.

**Available metrics**

Define the class, or label, set as

$$L = \{\ell_0, \ell_1, \ldots, \ell_{M-1} \} $$

The true output vector $\mathbf{y}$ consists of $N$ elements

$$\mathbf{y}_0, \mathbf{y}_1, \ldots, \mathbf{y}_{N-1} \in L $$

A multiclass prediction algorithm generates a prediction vector $\hat{\mathbf{y}}$ of $N$ elements

$$\hat{\mathbf{y}}_0, \hat{\mathbf{y}}_1, \ldots, \hat{\mathbf{y}}_{N-1} \in L $$

For this section, a modified delta function $\hat{\delta}(x)$ will prove useful

$$\hat{\delta}(x) = \begin{cases}1 & \text{if $x = 0$}, \\ 0 & \text{otherwise}.\end{cases}$$

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Confusion Matrix</td>
      <td>
        $C_{ij} = \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_i) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_j)\\ \\
         \left( \begin{array}{ccc}
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_1) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_1) & \ldots &
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_1) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_N) \\
         \vdots & \ddots & \vdots \\
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_N) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_1) & \ldots &
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_N) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_N)
         \end{array} \right)$
      </td>
    </tr>
    <tr>
      <td>Overall Precision</td>
      <td>$PPV = \frac{TP}{TP + FP} = \frac{1}{N}\sum_{i=0}^{N-1} \hat{\delta}\left(\hat{\mathbf{y}}_i -
        \mathbf{y}_i\right)$</td>
    </tr>
    <tr>
      <td>Overall Recall</td>
      <td>$TPR = \frac{TP}{TP + FN} = \frac{1}{N}\sum_{i=0}^{N-1} \hat{\delta}\left(\hat{\mathbf{y}}_i -
        \mathbf{y}_i\right)$</td>
    </tr>
    <tr>
      <td>Overall F1-measure</td>
      <td>$F1 = 2 \cdot \left(\frac{PPV \cdot TPR}
          {PPV + TPR}\right)$</td>
    </tr>
    <tr>
      <td>Precision by label</td>
      <td>$PPV(\ell) = \frac{TP}{TP + FP} =
          \frac{\sum_{i=0}^{N-1} \hat{\delta}(\hat{\mathbf{y}}_i - \ell) \cdot \hat{\delta}(\mathbf{y}_i - \ell)}
          {\sum_{i=0}^{N-1} \hat{\delta}(\hat{\mathbf{y}}_i - \ell)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td>
      <td>$TPR(\ell)=\frac{TP}{P} =
          \frac{\sum_{i=0}^{N-1} \hat{\delta}(\hat{\mathbf{y}}_i - \ell) \cdot \hat{\delta}(\mathbf{y}_i - \ell)}
          {\sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i - \ell)}$</td>
    </tr>
    <tr>
      <td>F-measure by label</td>
      <td>$F(\beta, \ell) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV(\ell) \cdot TPR(\ell)}
          {\beta^2 \cdot PPV(\ell) + TPR(\ell)}\right)$</td>
    </tr>
    <tr>
      <td>Weighted precision</td>
      <td>$PPV_{w}= \frac{1}{N} \sum\nolimits_{\ell \in L} PPV(\ell)
          \cdot \sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i-\ell)$</td>
    </tr>
    <tr>
      <td>Weighted recall</td>
      <td>$TPR_{w}= \frac{1}{N} \sum\nolimits_{\ell \in L} TPR(\ell)
          \cdot \sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i-\ell)$</td>
    </tr>
    <tr>
      <td>Weighted F-measure</td>
      <td>$F_{w}(\beta)= \frac{1}{N} \sum\nolimits_{\ell \in L} F(\beta, \ell)
          \cdot \sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i-\ell)$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">
The following code snippets illustrate how to load a sample dataset, train a multiclass classification algorithm on
the data, and evaluate the performance of the algorithm by several multiclass classification evaluation metrics.

<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_multiclass_classification_data.txt")

// Split data into training (60%) and test (40%)
val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
training.cache()

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(3)
  .run(training)

// Compute raw scores on the test set
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object
val metrics = new MulticlassMetrics(predictionAndLabels)

// Confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)

// Overall Statistics
val precision = metrics.precision
val recall = metrics.recall // same as true positive rate
val f1Score = metrics.fMeasure
println("Summary Statistics")
println(s"Precision = $precision")
println(s"Recall = $recall")
println(s"F1 Score = $f1Score")

// Precision by label
val labels = metrics.labels
labels.foreach { l =>
    println(s"Precision($l) = " + metrics.precision(l))
}

// Recall by label
labels.foreach { l =>
    println(s"Recall($l) = " + metrics.recall(l))
}

// False positive rate by label
labels.foreach { l =>
    println(s"FPR($l) = " + metrics.falsePositiveRate(l))
}

// F-measure by label
labels.foreach { l =>
    println(s"F1-Score($l) = " + metrics.fMeasure(l))
}

// Weighted stats
println(s"Weighted precision: ${metrics.weightedPrecision}")
println(s"Weighted recall: ${metrics.weightedRecall}")
println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class MulticlassClassification {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Multiclass Classification Metrics");
    SparkContext sc = new SparkContext(conf);
    String path = "data/mllib/sample_multiclass_classification_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    JavaRDD<LabeledPoint> test = splits[1];

    // Run training algorithm to build the model.
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(training.rdd());

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double prediction = model.predict(p.features());
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      }
    );

    // Get evaluation metrics.
    MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

    // Confusion matrix
    Matrix confusion = metrics.confusionMatrix();
    System.out.println("Confusion matrix: \n" + confusion);

    // Overall statistics
    System.out.println("Precision = " + metrics.precision());
    System.out.println("Recall = " + metrics.recall());
    System.out.println("F1 Score = " + metrics.fMeasure());

    // Stats by labels
    for (int i = 0; i < metrics.labels().length; i++) {
        System.out.format("Class %f precision = %f\n", metrics.labels()[i], metrics.precision(metrics.labels()[i]));
        System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(metrics.labels()[i]));
        System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(metrics.labels()[i]));
    }

    //Weighted stats
    System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
    System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
    System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
    System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());

    // Save and load model
    model.save(sc, "myModelPath");
    LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc, "myModelPath");
  }
}

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics

# Load training data in LIBSVM format
data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_multiclass_classification_data.txt")

# Split data into training (60%) and test (40%)
training, test = data.randomSplit([0.6, 0.4], seed = 11L)
training.cache()

# Run training algorithm to build the model
model = LogisticRegressionWithLBFGS.train(training, numClasses=3)

# Compute raw scores on the test set
predictionAndLabels = test.map(lambda lp: (float(model.predict(lp.features)), lp.label))

# Instantiate metrics object
metrics = MulticlassMetrics(predictionAndLabels)

# Overall statistics
precision = metrics.precision()
recall = metrics.recall()
f1Score = metrics.fMeasure()
print("Summary Stats")
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)

# Statistics by class
labels = data.map(lambda lp: lp.label).distinct().collect()
for label in sorted(labels):
    print("Class %s precision = %s" % (label, metrics.precision(label)))
    print("Class %s recall = %s" % (label, metrics.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))

# Weighted stats
print("Weighted recall = %s" % metrics.weightedRecall)
print("Weighted precision = %s" % metrics.weightedPrecision)
print("Weighted F(1) Score = %s" % metrics.weightedFMeasure())
print("Weighted F(0.5) Score = %s" % metrics.weightedFMeasure(beta=0.5))
print("Weighted false positive rate = %s" % metrics.weightedFalsePositiveRate)
{% endhighlight %}

</div>
</div>

### Multilabel classification

A [multilabel classification](https://en.wikipedia.org/wiki/Multi-label_classification) problem involves mapping
each sample in a dataset to a set of class labels. In this type of classification problem, the labels are not
mutually exclusive. For example, when classifying a set of news articles into topics, a single article might be both
science and politics.

Because the labels are not mutually exclusive, the predictions and true labels are now vectors of label *sets*, rather
than vectors of labels. Multilabel metrics, therefore, extend the fundamental ideas of precision, recall, etc. to
operations on sets. For example, a true positive for a given class now occurs when that class exists in the predicted
set and it exists in the true label set, for a specific data point.

**Available metrics**

Here we define a set $D$ of $N$ documents

$$D = \left\{d_0, d_1, ..., d_{N-1}\right\}$$

Define $L_0, L_1, ..., L_{N-1}$ to be a family of label sets and $P_0, P_1, ..., P_{N-1}$
to be a family of prediction sets where $L_i$ and $P_i$ are the label set and prediction set, respectively, that
correspond to document $d_i$.

The set of all unique labels is given by

$$L = \bigcup_{k=0}^{N-1} L_k$$

The following definition of indicator function $I_A(x)$ on a set $A$ will be necessary

$$I_A(x) = \begin{cases}1 & \text{if $x \in A$}, \\ 0 & \text{otherwise}.\end{cases}$$

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Precision</td><td>$\frac{1}{N} \sum_{i=0}^{N-1} \frac{\left|P_i \cap L_i\right|}{\left|P_i\right|}$</td>
    </tr>
    <tr>
      <td>Recall</td><td>$\frac{1}{N} \sum_{i=0}^{N-1} \frac{\left|L_i \cap P_i\right|}{\left|L_i\right|}$</td>
    </tr>
    <tr>
      <td>Accuracy</td>
      <td>
        $\frac{1}{N} \sum_{i=0}^{N - 1} \frac{\left|L_i \cap P_i \right|}
        {\left|L_i\right| + \left|P_i\right| - \left|L_i \cap P_i \right|}$
      </td>
    </tr>
    <tr>
      <td>Precision by label</td><td>$PPV(\ell)=\frac{TP}{TP + FP}=
          \frac{\sum_{i=0}^{N-1} I_{P_i}(\ell) \cdot I_{L_i}(\ell)}
          {\sum_{i=0}^{N-1} I_{P_i}(\ell)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td><td>$TPR(\ell)=\frac{TP}{P}=
          \frac{\sum_{i=0}^{N-1} I_{P_i}(\ell) \cdot I_{L_i}(\ell)}
          {\sum_{i=0}^{N-1} I_{L_i}(\ell)}$</td>
    </tr>
    <tr>
      <td>F1-measure by label</td><td>$F1(\ell) = 2
                            \cdot \left(\frac{PPV(\ell) \cdot TPR(\ell)}
                            {PPV(\ell) + TPR(\ell)}\right)$</td>
    </tr>
    <tr>
      <td>Hamming Loss</td>
      <td>
        $\frac{1}{N \cdot \left|L\right|} \sum_{i=0}^{N - 1} \left|L_i\right| + \left|P_i\right| - 2\left|L_i
          \cap P_i\right|$
      </td>
    </tr>
    <tr>
      <td>Subset Accuracy</td>
      <td>$\frac{1}{N} \sum_{i=0}^{N-1} I_{\{L_i\}}(P_i)$</td>
    </tr>
    <tr>
      <td>F1 Measure</td>
      <td>$\frac{1}{N} \sum_{i=0}^{N-1} 2 \frac{\left|P_i \cap L_i\right|}{\left|P_i\right| \cdot \left|L_i\right|}$</td>
    </tr>
    <tr>
      <td>Micro precision</td>
      <td>$\frac{TP}{TP + FP}=\frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}
          {\sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|P_i - L_i\right|}$</td>
    </tr>
    <tr>
      <td>Micro recall</td>
      <td>$\frac{TP}{TP + FN}=\frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}
        {\sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|L_i - P_i\right|}$</td>
    </tr>
    <tr>
      <td>Micro F1 Measure</td>
      <td>
        $2 \cdot \frac{TP}{2 \cdot TP + FP + FN}=2 \cdot \frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}{2 \cdot
        \sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|L_i - P_i\right| + \sum_{i=0}^{N-1}
        \left|P_i - L_i\right|}$
      </td>
    </tr>
  </tbody>
</table>

**Examples**

The following code snippets illustrate how to evaluate the performance of a multilabel classifer. The examples
use the fake prediction and label data for multilabel classification that is shown below.

Document predictions:

* doc 0 - predict 0, 1 - class 0, 2
* doc 1 - predict 0, 2 - class 0, 1
* doc 2 - predict none - class 0
* doc 3 - predict 2 - class 2
* doc 4 - predict 2, 0 - class 2, 0
* doc 5 - predict 0, 1, 2 - class 0, 1
* doc 6 - predict 1 - class 1, 2

Predicted classes:

* class 0 - doc 0, 1, 4, 5 (total 4)
* class 1 - doc 0, 5, 6 (total 3)
* class 2 - doc 1, 3, 4, 5 (total 4)

True classes:

* class 0 - doc 0, 1, 2, 4, 5 (total 5)
* class 1 - doc 1, 5, 6 (total 3)
* class 2 - doc 0, 3, 4, 6 (total 4)

<div class="codetabs">

<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.rdd.RDD;

val scoreAndLabels: RDD[(Array[Double], Array[Double])] = sc.parallelize(
  Seq((Array(0.0, 1.0), Array(0.0, 2.0)),
    (Array(0.0, 2.0), Array(0.0, 1.0)),
    (Array(), Array(0.0)),
    (Array(2.0), Array(2.0)),
    (Array(2.0, 0.0), Array(2.0, 0.0)),
    (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
    (Array(1.0), Array(1.0, 2.0))), 2)

// Instantiate metrics object
val metrics = new MultilabelMetrics(scoreAndLabels)

// Summary stats
println(s"Recall = ${metrics.recall}")
println(s"Precision = ${metrics.precision}")
println(s"F1 measure = ${metrics.f1Measure}")
println(s"Accuracy = ${metrics.accuracy}")

// Individual label stats
metrics.labels.foreach(label => println(s"Class $label precision = ${metrics.precision(label)}"))
metrics.labels.foreach(label => println(s"Class $label recall = ${metrics.recall(label)}"))
metrics.labels.foreach(label => println(s"Class $label F1-score = ${metrics.f1Measure(label)}"))

// Micro stats
println(s"Micro recall = ${metrics.microRecall}")
println(s"Micro precision = ${metrics.microPrecision}")
println(s"Micro F1 measure = ${metrics.microF1Measure}")

// Hamming loss
println(s"Hamming loss = ${metrics.hammingLoss}")

// Subset accuracy
println(s"Subset accuracy = ${metrics.subsetAccuracy}")

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.evaluation.MultilabelMetrics;
import org.apache.spark.SparkConf;
import java.util.Arrays;
import java.util.List;

public class MultilabelClassification {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Multilabel Classification Metrics");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<double[], double[]>> data = Arrays.asList(
        new Tuple2<double[], double[]>(new double[]{0.0, 1.0}, new double[]{0.0, 2.0}),
        new Tuple2<double[], double[]>(new double[]{0.0, 2.0}, new double[]{0.0, 1.0}),
        new Tuple2<double[], double[]>(new double[]{}, new double[]{0.0}),
        new Tuple2<double[], double[]>(new double[]{2.0}, new double[]{2.0}),
        new Tuple2<double[], double[]>(new double[]{2.0, 0.0}, new double[]{2.0, 0.0}),
        new Tuple2<double[], double[]>(new double[]{0.0, 1.0, 2.0}, new double[]{0.0, 1.0}),
        new Tuple2<double[], double[]>(new double[]{1.0}, new double[]{1.0, 2.0})
        );
    JavaRDD<Tuple2<double[], double[]>> scoreAndLabels = sc.parallelize(data);

    // Instantiate metrics object
    MultilabelMetrics metrics = new MultilabelMetrics(scoreAndLabels.rdd());

    // Summary stats
    System.out.format("Recall = %f\n", metrics.recall());
    System.out.format("Precision = %f\n", metrics.precision());
    System.out.format("F1 measure = %f\n", metrics.f1Measure());
    System.out.format("Accuracy = %f\n", metrics.accuracy());

    // Stats by labels
    for (int i = 0; i < metrics.labels().length - 1; i++) {
        System.out.format("Class %1.1f precision = %f\n", metrics.labels()[i], metrics.precision(metrics.labels()[i]));
        System.out.format("Class %1.1f recall = %f\n", metrics.labels()[i], metrics.recall(metrics.labels()[i]));
        System.out.format("Class %1.1f F1 score = %f\n", metrics.labels()[i], metrics.f1Measure(metrics.labels()[i]));
    }

    // Micro stats
    System.out.format("Micro recall = %f\n", metrics.microRecall());
    System.out.format("Micro precision = %f\n", metrics.microPrecision());
    System.out.format("Micro F1 measure = %f\n", metrics.microF1Measure());

    // Hamming loss
    System.out.format("Hamming loss = %f\n", metrics.hammingLoss());

    // Subset accuracy
    System.out.format("Subset accuracy = %f\n", metrics.subsetAccuracy());

  }
}

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
from pyspark.mllib.evaluation import MultilabelMetrics

scoreAndLabels = sc.parallelize([
    ([0.0, 1.0], [0.0, 2.0]),
    ([0.0, 2.0], [0.0, 1.0]),
    ([], [0.0]),
    ([2.0], [2.0]),
    ([2.0, 0.0], [2.0, 0.0]),
    ([0.0, 1.0, 2.0], [0.0, 1.0]),
    ([1.0], [1.0, 2.0])])

# Instantiate metrics object
metrics = MultilabelMetrics(scoreAndLabels)

# Summary stats
print("Recall = %s" % metrics.recall())
print("Precision = %s" % metrics.precision())
print("F1 measure = %s" % metrics.f1Measure())
print("Accuracy = %s" % metrics.accuracy)

# Individual label stats
labels = scoreAndLabels.flatMap(lambda x: x[1]).distinct().collect()
for label in labels:
    print("Class %s precision = %s" % (label, metrics.precision(label)))
    print("Class %s recall = %s" % (label, metrics.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics.f1Measure(label)))

# Micro stats
print("Micro precision = %s" % metrics.microPrecision)
print("Micro recall = %s" % metrics.microRecall)
print("Micro F1 measure = %s" % metrics.microF1Measure)

# Hamming loss
print("Hamming loss = %s" % metrics.hammingLoss)

# Subset accuracy
print("Subset accuracy = %s" % metrics.subsetAccuracy)

{% endhighlight %}

</div>
</div>

### Ranking systems

The role of a ranking algorithm (often thought of as a [recommender system](https://en.wikipedia.org/wiki/Recommender_system))
is to return to the user a set of relevant items or documents based on some training data. The definition of relevance
may vary and is usually application specific. Ranking system metrics aim to quantify the effectiveness of these
rankings or recommendations in various contexts. Some metrics compare a set of recommended documents to a ground truth
set of relevant documents, while other metrics may incorporate numerical ratings explicitly.

**Available metrics**

A ranking system usually deals with a set of $M$ users

$$U = \left\{u_0, u_1, ..., u_{M-1}\right\}$$

Each user ($u_i$) having a set of $N$ ground truth relevant documents

$$D_i = \left\{d_0, d_1, ..., d_{N-1}\right\}$$

And a list of $Q$ recommended documents, in order of decreasing relevance

$$R_i = \left[r_0, r_1, ..., r_{Q-1}\right]$$

The goal of the ranking system is to produce the most relevant set of documents for each user. The relevance of the
sets and the effectiveness of the algorithms can be measured using the metrics listed below.

It is necessary to define a function which, provided a recommended document and a set of ground truth relevant
documents, returns a relevance score for the recommended document.

$$rel_D(r) = \begin{cases}1 & \text{if $r \in D$}, \\ 0 & \text{otherwise}.\end{cases}$$

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th><th>Notes</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>
        Precision at k
      </td>
      <td>
        $p(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{k} \sum_{j=0}^{\text{min}(\left|D\right|, k) - 1} rel_{D_i}(R_i(j))}$
      </td>
      <td>
        <a href="https://en.wikipedia.org/wiki/Information_retrieval#Precision_at_K">Precision at k</a> is a measure of
         how many of the first k recommended documents are in the set of true relevant documents averaged across all
         users. In this metric, the order of the recommendations is not taken into account.
      </td>
    </tr>
    <tr>
      <td>Mean Average Precision</td>
      <td>
        $MAP=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{\left|D_i\right|} \sum_{j=0}^{Q-1} \frac{rel_{D_i}(R_i(j))}{j + 1}}$
      </td>
      <td>
        <a href="https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision">MAP</a> is a measure of how
         many of the recommended documents are in the set of true relevant documents, where the
        order of the recommendations is taken into account (i.e. penalty for highly relevant documents is higher).
      </td>
    </tr>
    <tr>
      <td>Normalized Discounted Cumulative Gain</td>
      <td>
        $NDCG(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{IDCG(D_i, k)}\sum_{j=0}^{n-1}
          \frac{rel_{D_i}(R_i(j))}{\text{ln}(j+1)}} \\
        \text{Where} \\
        \hspace{5 mm} n = \text{min}\left(\text{max}\left(|R_i|,|D_i|\right),k\right) \\
        \hspace{5 mm} IDCG(D, k) = \sum_{j=0}^{\text{min}(\left|D\right|, k) - 1} \frac{1}{\text{ln}(j+1)}$
      </td>
      <td>
        <a href="https://en.wikipedia.org/wiki/Information_retrieval#Discounted_cumulative_gain">NDCG at k</a> is a
        measure of how many of the first k recommended documents are in the set of true relevant documents averaged
        across all users. In contrast to precision at k, this metric takes into account the order of the recommendations
        (documents are assumed to be in order of decreasing relevance).
      </td>
    </tr>
  </tbody>
</table>

**Examples**

The following code snippets illustrate how to load a sample dataset, train an alternating least squares recommendation
model on the data, and evaluate the performance of the recommender by several ranking metrics. A brief summary of the
methodology is provided below.

MovieLens ratings are on a scale of 1-5:

 * 5: Must see
 * 4: Will enjoy
 * 3: It's okay
 * 2: Fairly bad
 * 1: Awful

So we should not recommend a movie if the predicted rating is less than 3.
To map ratings to confidence scores, we use:

 * 5 -> 2.5
 * 4 -> 1.5
 * 3 -> 0.5
 * 2 -> -0.5
 * 1 -> -1.5.

This mappings means unobserved entries are generally between It's okay and Fairly bad. The semantics of 0 in this
expanded world of non-positive weights are "the same as never having interacted at all."

<div class="codetabs">

<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.evaluation.{RegressionMetrics, RankingMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

// Read in the ratings data
val ratings = sc.textFile("data/mllib/sample_movielens_data.txt").map { line =>
  val fields = line.split("::")
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
}.cache()

// Map ratings to 1 or 0, 1 indicating a movie that should be recommended
val binarizedRatings = ratings.map(r => Rating(r.user, r.product, if (r.rating > 0) 1.0 else 0.0)).cache()

// Summarize ratings
val numRatings = ratings.count()
val numUsers = ratings.map(_.user).distinct().count()
val numMovies = ratings.map(_.product).distinct().count()
println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

// Build the model
val numIterations = 10
val rank = 10
val lambda = 0.01
val model = ALS.train(ratings, rank, numIterations, lambda)

// Define a function to scale ratings from 0 to 1
def scaledRating(r: Rating): Rating = {
  val scaledRating = math.max(math.min(r.rating, 1.0), 0.0)
  Rating(r.user, r.product, scaledRating)
}

// Get sorted top ten predictions for each user and then scale from [0, 1]
val userRecommended = model.recommendProductsForUsers(10).map{ case (user, recs) =>
  (user, recs.map(scaledRating))
}

// Assume that any movie a user rated 3 or higher (which maps to a 1) is a relevant document
// Compare with top ten most relevant documents
val userMovies = binarizedRatings.groupBy(_.user)
val relevantDocuments = userMovies.join(userRecommended).map{ case (user, (actual, predictions)) =>
  (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray)
}

// Instantiate metrics object
val metrics = new RankingMetrics(relevantDocuments)

// Precision at K
Array(1, 3, 5).foreach{ k =>
  println(s"Precision at $k = ${metrics.precisionAt(k)}")
}

// Mean average precision
println(s"Mean average precision = ${metrics.meanAveragePrecision}")

// Normalized discounted cumulative gain
Array(1, 3, 5).foreach{ k =>
  println(s"NDCG at $k = ${metrics.ndcgAt(k)}")
}

// Get predictions for each data point
val allPredictions = model.predict(ratings.map(r => (r.user, r.product))).map(r => ((r.user, r.product), r.rating))
val allRatings = ratings.map(r => ((r.user, r.product), r.rating))
val predictionsAndLabels = allPredictions.join(allRatings).map{ case ((user, product), (predicted, actual)) =>
  (predicted, actual)
}

// Get the RMSE using regression metrics
val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

// R-squared
println(s"R-squared = ${regressionMetrics.r2}")

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import java.util.*;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.evaluation.RankingMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;

// Read in the ratings data
public class Ranking {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Ranking Metrics");
    JavaSparkContext sc = new JavaSparkContext(conf);
    String path = "data/mllib/sample_movielens_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Rating> ratings = data.map(
      new Function<String, Rating>() {
        public Rating call(String line) {
          String[] parts = line.split("::");
          return new Rating(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Double.parseDouble(parts[2]) - 2.5);
        }
      }
    );
    ratings.cache();

    // Train an ALS model
    final MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), 10, 10, 0.01);

    // Get top 10 recommendations for every user and scale ratings from 0 to 1
    JavaRDD<Tuple2<Object, Rating[]>> userRecs = model.recommendProductsForUsers(10).toJavaRDD();
    JavaRDD<Tuple2<Object, Rating[]>> userRecsScaled = userRecs.map(
      new Function<Tuple2<Object, Rating[]>, Tuple2<Object, Rating[]>>() {
        public Tuple2<Object, Rating[]> call(Tuple2<Object, Rating[]> t) {
          Rating[] scaledRatings = new Rating[t._2().length];
          for (int i = 0; i < scaledRatings.length; i++) {
            double newRating = Math.max(Math.min(t._2()[i].rating(), 1.0), 0.0);
            scaledRatings[i] = new Rating(t._2()[i].user(), t._2()[i].product(), newRating);
          }
          return new Tuple2<Object, Rating[]>(t._1(), scaledRatings);
        }
      }
    );
    JavaPairRDD<Object, Rating[]> userRecommended = JavaPairRDD.fromJavaRDD(userRecsScaled);

    // Map ratings to 1 or 0, 1 indicating a movie that should be recommended
    JavaRDD<Rating> binarizedRatings = ratings.map(
      new Function<Rating, Rating>() {
        public Rating call(Rating r) {
          double binaryRating;
          if (r.rating() > 0.0) {
            binaryRating = 1.0;
          }
          else {
            binaryRating = 0.0;
          }
          return new Rating(r.user(), r.product(), binaryRating);
        }
      }
    );

    // Group ratings by common user
    JavaPairRDD<Object, Iterable<Rating>> userMovies = binarizedRatings.groupBy(
      new Function<Rating, Object>() {
        public Object call(Rating r) {
          return r.user();
        }
      }
    );

    // Get true relevant documents from all user ratings
    JavaPairRDD<Object, List<Integer>> userMoviesList = userMovies.mapValues(
      new Function<Iterable<Rating>, List<Integer>>() {
        public List<Integer> call(Iterable<Rating> docs) {
          List<Integer> products = new ArrayList<Integer>();
          for (Rating r : docs) {
            if (r.rating() > 0.0) {
              products.add(r.product());
            }
          }
          return products;
        }
      }
    );

    // Extract the product id from each recommendation
    JavaPairRDD<Object, List<Integer>> userRecommendedList = userRecommended.mapValues(
      new Function<Rating[], List<Integer>>() {
        public List<Integer> call(Rating[] docs) {
          List<Integer> products = new ArrayList<Integer>();
          for (Rating r : docs) {
            products.add(r.product());
          }
          return products;
        }
      }
    );
    JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = userMoviesList.join(userRecommendedList).values();

    // Instantiate the metrics object
    RankingMetrics metrics = RankingMetrics.of(relevantDocs);

    // Precision and NDCG at k
    Integer[] kVector = {1, 3, 5};
    for (Integer k : kVector) {
      System.out.format("Precision at %d = %f\n", k, metrics.precisionAt(k));
      System.out.format("NDCG at %d = %f\n", k, metrics.ndcgAt(k));
    }

    // Mean average precision
    System.out.format("Mean average precision = %f\n", metrics.meanAveragePrecision());

    // Evaluate the model using numerical ratings and regression metrics
    JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
      new Function<Rating, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(Rating r) {
          return new Tuple2<Object, Object>(r.user(), r.product());
        }
      }
    );
    JavaPairRDD<Tuple2<Integer, Integer>, Object> predictions = JavaPairRDD.fromJavaRDD(
      model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Object>>() {
          public Tuple2<Tuple2<Integer, Integer>, Object> call(Rating r){
            return new Tuple2<Tuple2<Integer, Integer>, Object>(
              new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
          }
        }
    ));
    JavaRDD<Tuple2<Object, Object>> ratesAndPreds =
      JavaPairRDD.fromJavaRDD(ratings.map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Object>>() {
          public Tuple2<Tuple2<Integer, Integer>, Object> call(Rating r){
            return new Tuple2<Tuple2<Integer, Integer>, Object>(
              new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
          }
        }
    )).join(predictions).values();

    // Create regression metrics object
    RegressionMetrics regressionMetrics = new RegressionMetrics(ratesAndPreds.rdd());

    // Root mean squared error
    System.out.format("RMSE = %f\n", regressionMetrics.rootMeanSquaredError());

    // R-squared
    System.out.format("R-squared = %f\n", regressionMetrics.r2());
  }
}

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics

#  Read in the ratings data
lines = sc.textFile("data/mllib/sample_movielens_data.txt")

def parseLine(line):
    fields = line.split("::")
    return Rating(int(fields[0]), int(fields[1]), float(fields[2]) - 2.5)
ratings = lines.map(lambda r: parseLine(r))

# Train a model on to predict user-product ratings
model = ALS.train(ratings, 10, 10, 0.01)

# Get predicted ratings on all existing user-product pairs
testData = ratings.map(lambda p: (p.user, p.product))
predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))

ratingsTuple = ratings.map(lambda r: ((r.user, r.product), r.rating))
scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

# Instantiate regression metrics to compare predicted and actual ratings
metrics = RegressionMetrics(scoreAndLabels)

# Root mean sqaured error
print("RMSE = %s" % metrics.rootMeanSquaredError)

# R-squared
print("R-squared = %s" % metrics.r2)

{% endhighlight %}

</div>
</div>

## Regression model evaluation

[Regression analysis](https://en.wikipedia.org/wiki/Regression_analysis) is used when predicting a continuous output
variable from a number of independent variables.

**Available metrics**

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Mean Squared Error (MSE)</td>
      <td>$MSE = \frac{\sum_{i=0}^{N-1} (\mathbf{y}_i - \hat{\mathbf{y}}_i)^2}{N}$</td>
    </tr>
    <tr>
      <td>Root Mean Squared Error (RMSE)</td>
      <td>$RMSE = \sqrt{\frac{\sum_{i=0}^{N-1} (\mathbf{y}_i - \hat{\mathbf{y}}_i)^2}{N}}$</td>
    </tr>
    <tr>
      <td>Mean Absoloute Error (MAE)</td>
      <td>$MAE=\sum_{i=0}^{N-1} \left|\mathbf{y}_i - \hat{\mathbf{y}}_i\right|$</td>
    </tr>
    <tr>
      <td>Coefficient of Determination $(R^2)$</td>
      <td>$R^2=1 - \frac{MSE}{\text{VAR}(\mathbf{y}) \cdot (N-1)}=1-\frac{\sum_{i=0}^{N-1}
        (\mathbf{y}_i - \hat{\mathbf{y}}_i)^2}{\sum_{i=0}^{N-1}(\mathbf{y}_i-\bar{\mathbf{y}})^2}$</td>
    </tr>
    <tr>
      <td>Explained Variance</td>
      <td>$1 - \frac{\text{VAR}(\mathbf{y} - \mathbf{\hat{y}})}{\text{VAR}(\mathbf{y})}$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">
The following code snippets illustrate how to load a sample dataset, train a linear regression algorithm on the data,
and evaluate the performance of the algorithm by several regression metrics.

<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.util.MLUtils

// Load the data
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_linear_regression_data.txt").cache()

// Build the model
val numIterations = 100
val model = LinearRegressionWithSGD.train(data, numIterations)

// Get predictions
val valuesAndPreds = data.map{ point =>
  val prediction = model.predict(point.features)
  (prediction, point.label)
}

// Instantiate metrics object
val metrics = new RegressionMetrics(valuesAndPreds)

// Squared error
println(s"MSE = ${metrics.meanSquaredError}")
println(s"RMSE = ${metrics.rootMeanSquaredError}")

// R-squared
println(s"R-squared = ${metrics.r2}")

// Mean absolute error
println(s"MAE = ${metrics.meanAbsoluteError}")

// Explained variance
println(s"Explained variance = ${metrics.explainedVariance}")

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.SparkConf;

public class LinearRegression {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Linear Regression Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load and parse the data
    String path = "data/mllib/sample_linear_regression_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<LabeledPoint> parsedData = data.map(
      new Function<String, LabeledPoint>() {
        public LabeledPoint call(String line) {
          String[] parts = line.split(" ");
          double[] v = new double[parts.length - 1];
          for (int i = 1; i < parts.length - 1; i++)
            v[i - 1] = Double.parseDouble(parts[i].split(":")[1]);
          return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
        }
      }
    );
    parsedData.cache();

    // Building the model
    int numIterations = 100;
    final LinearRegressionModel model =
      LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

    // Evaluate model on training examples and compute training error
    JavaRDD<Tuple2<Object, Object>> valuesAndPreds = parsedData.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint point) {
          double prediction = model.predict(point.features());
          return new Tuple2<Object, Object>(prediction, point.label());
        }
      }
    );

    // Instantiate metrics object
    RegressionMetrics metrics = new RegressionMetrics(valuesAndPreds.rdd());

    // Squared error
    System.out.format("MSE = %f\n", metrics.meanSquaredError());
    System.out.format("RMSE = %f\n", metrics.rootMeanSquaredError());

    // R-squared
    System.out.format("R Squared = %f\n", metrics.r2());

    // Mean absolute error
    System.out.format("MAE = %f\n", metrics.meanAbsoluteError());

    // Explained variance
    System.out.format("Explained Variance = %f\n", metrics.explainedVariance());

    // Save and load model
    model.save(sc.sc(), "myModelPath");
    LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModelPath");
  }
}

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.linalg import DenseVector

# Load and parse the data
def parsePoint(line):
    values = line.split()
    return LabeledPoint(float(values[0]), DenseVector([float(x.split(':')[1]) for x in values[1:]]))

data = sc.textFile("data/mllib/sample_linear_regression_data.txt")
parsedData = data.map(parsePoint)

# Build the model
model = LinearRegressionWithSGD.train(parsedData)

# Get predictions
valuesAndPreds = parsedData.map(lambda p: (float(model.predict(p.features)), p.label))

# Instantiate metrics object
metrics = RegressionMetrics(valuesAndPreds)

# Squared Error
print("MSE = %s" % metrics.meanSquaredError)
print("RMSE = %s" % metrics.rootMeanSquaredError)

# R-squared
print("R-squared = %s" % metrics.r2)

# Mean absolute error
print("MAE = %s" % metrics.meanAbsoluteError)

# Explained variance
print("Explained variance = %s" % metrics.explainedVariance)

{% endhighlight %}

</div>
</div>