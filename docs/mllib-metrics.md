---
layout: global
title: Evaluation Metrics - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Evaluation Metrics
---

* Table of contents
{:toc}


`\[
\newcommand{\R}{\mathbb{R}}
\newcommand{\E}{\mathbb{E}}
\newcommand{\x}{\mathbf{x}}
\newcommand{\y}{\mathbf{y}}
\newcommand{\wv}{\mathbf{w}}
\newcommand{\av}{\mathbf{\alpha}}
\newcommand{\bv}{\mathbf{b}}
\newcommand{\N}{\mathbb{N}}
\newcommand{\id}{\mathbf{I}}
\newcommand{\ind}{\mathbf{1}}
\newcommand{\0}{\mathbf{0}}
\newcommand{\unit}{\mathbf{e}}
\newcommand{\one}{\mathbf{1}}
\newcommand{\zero}{\mathbf{0}}
\]`

## Binary Classification

Binary classifiers are used to separate the elements of a given set into two
separate groups and is a special case of multiclass classification. Most binary
classification metrics can be generalized to multiclass classification metrics.

### Metrics

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Precision (Postive Predictive Value)</td><td>$PPV=\frac{TP}{TP + FP}$</td>
    </tr>
    <tr>
      <td>Recall (True Positive Rate)</td><td>$TPR=\frac{TP}{P}=\frac{TP}{TP + FN}$</td>
    </tr>
    <tr>
      <td>F-measure</td><td>$F(\beta) = \left(1 + \beta^2\right)
                            \cdot \left(\frac{precision \cdot recall}
                            {\beta^2 \cdot precision + recall}\right)$</td>
    </tr>
    <tr>
      <td>Receiver Operating Characteristic (ROC)</td><td>$FPR(T)=\int^\infty_{T} P_0(T)\,dT\\
                                                    TPR(T)=\int^\infty_{T} P_1(T)\,dT$</td>
    </tr>
    <tr>
      <td>Area Under ROC Curve</td><td>$AUROC=\int^1_{0} \frac{TP}{P} d\left(\frac{FP}{N}\right)$</td>
    </tr>
    <tr>
      <td>Area Under Precision-Recall Curve</td><td>$AUPRC=\int^1_{0} \frac{TP}{TP+FP} d\left(\frac{TP}{P}\right)$</td>
    </tr>
  </tbody>
</table>


**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippets illustrate how to load a sample dataset, train a
binary classification algorithm on the data, and evaluate the performance of
the algorithm by several binary evaluation metrics.

{% highlight scala %}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_binary_classification_data.txt")

// Split data into training (60%) and test (40%)
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(10)
  .run(training)

// Compute raw scores on the test set
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object
val metrics = new BinaryClassificationMetrics(predictionAndLabels)

// Precision by threshold
val precision = metrics.precisionByThreshold
precision.foreach(x => printf("Threshold: %1.2f, Precision: %1.2f\n", x._1, x._2))

// Recall by threshold
val recall = metrics.precisionByThreshold
recall.foreach(x => printf("Threshold: %1.2f, Recall: %1.2f\n", x._1, x._2))

// Precision-Recall Curve
val PRC = metrics.pr

// F-measure
val f1Score = metrics.fMeasureByThreshold
f1Score.foreach(x => printf("Threshold: %1.2f, F-score: %1.2f, Beta = 1\n", x._1, x._2))

val beta = 0.5
val fScore = metrics.fMeasureByThreshold(beta)
fScore.foreach(x => printf("Threshold: %1.2f, F-score: %1.2f, Beta = 0.5\n", x._1, x._2))

// AUPRC
val auPRC = metrics.areaUnderPR
println("Area under precision-recall curve = " + auPRC)

// Compute thresholds used in ROC and PR curves
val distinctThresholds = predictionAndLabels.map(_._1).distinct().collect().sortWith(_ > _)
val thresholds = Array(1.0) ++ distinctThresholds ++ Array(0.0)

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
import java.util.Collections;
import java.util.Arrays;
import java.util.List;

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
      .setNumClasses(10)
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
    // TODO: calc thresholds
    // JavaRDD<Double> distinctThresholds = predictionAndLabels.map(
    //   new Function<Tuple2<Object, Object>, Double>() {
    //     public Double call (Tuple2<Object, Object> t) {
    //       return new Double(t._1().toString());
    //     }
    //   }
    // ).distinct();
    // // Double[] sortedThresholds = Arrays.sort(distinctThresholds.toArray(), Collections.reverseOrder());
    // List<Double> srtd = distinctThresholds.toArray();
    // System.out.println(distinctThresholds.toArray());
    // System.out.println(srtd.getClass().getName());

    // ROC Curve
    JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
    System.out.println("ROC curve: " + roc.toArray());

    // AUPRC
    System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

    // AUROC
    System.out.println("Area under ROC = " + metrics.areaUnderROC());

    // Save and load model
    // model.save(sc, "myModelPath");
    // LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc, "myModelPath");
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
splits = data.randomSplit([0.6, 0.4], seed = 11L)
training = splits[0].cache()
test = splits[1]

# Run training algorithm to build the model
model = LogisticRegressionWithLBFGS.train(training)

# Compute raw scores on the test set
predictionAndLabels = test.map(lambda lp: (float(model.predict(lp.features)), lp.label))

# Instantiate metrics object
metrics = BinaryClassificationMetrics(predictionAndLabels)

# Area under precision-recall curve
print "Area under PR = %1.2f" % metrics.areaUnderPR

# Area under ROC curve
print "Area under ROC = %1.2f" % metrics.areaUnderROC

{% endhighlight %}

</div>
</div>


## Multiclass Classification

A multiclass classification describes a classification problem where there are
$M \gt 2$ possible labels for each data point (the case where $M=2$ is the
binary classification problem).

The label set is given by

$$L = \{\ell_0, \ell_1, \ldots, \ell_{M-1} \} $$

The true output vector $\mathbf{y}$ consists of N elements

$$\mathbf{y}_0, \mathbf{y}_1, \ldots, \mathbf{y}_{N-1} \in L $$

A multiclass prediction algorithm generates a prediction vector $\hat{\mathbf{y}}$ of N elements

$$\hat{\mathbf{y}}_0, \hat{\mathbf{y}}_1, \ldots, \hat{\mathbf{y}}_{N-1} \in L $$

NOTE: The convention $\mathbf{y}^{(\ell)}$ will be used to denote the subset of the elements of $y$ which
are equal to $\ell$

TODO: notation needs reviewing
<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Confusion Matrix</td>
      <td>
        $\left( \begin{array}{ccc}
         \mathbf{y}^{(1)}=\hat{\mathbf{y}}^{(1)} & \ldots & \mathbf{y}^{(1)}=\hat{\mathbf{y}}^{(N)} \\
         \vdots & \ddots & \vdots \\
         \mathbf{y}^{(N)}=\hat{\mathbf{y}}^{(1)} & \ldots & \mathbf{y}^{(N)}=\hat{\mathbf{y}}^{(N)} \end{array} \right)$
      </td>
    </tr>
    <tr>
      <td>Overall Precision</td><td>$precision=\frac{TP}{TP + FP}=
          \frac{1}{N}\sum_{i=0}^{N-1} \hat{\mathbf{y}}_i=\mathbf{y}_i$</td>
    </tr>
    <tr>
      <td>Overall Recall</td><td>$recall=\frac{TP}{TP + FN}=
          \frac{1}{N}\sum_{i=0}^{N-1} \hat{\mathbf{y}}_i=\mathbf{y}_i$</td>
    </tr>
    <tr>
      <td>Overall F-measure</td><td>$F(\beta) = \left(1 + \beta^2\right)
                            \cdot \left(\frac{precision \cdot recall}
                            {\beta^2 \cdot precision + recall}\right)$</td>
    </tr>
    <tr>
      <td>Precision by label</td><td>$precision(\ell)=\frac{TP}{TP + FP}=
          \frac{\sum_{i=0}^{N-1} (\hat{\mathbf{y}}_i=\ell)\:\&\&\:(\mathbf{y}_i=\ell)}
          {\sum_{i=0}^{N-1} (\hat{\mathbf{y}}_i=\ell)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td><td>$recall(\ell)=\frac{TP}{P}=
          \frac{\sum_{i=0}^{N-1} (\hat{\mathbf{y}}_i=\ell)\:\&\&\:(\mathbf{y}_i=\ell)}
          {\sum_{i=0}^{N-1} (\mathbf{y}_i=\ell)}$</td>
    </tr>
    <tr>
      <td>F-measure by label</td><td>$F(\beta, \ell) = \left(1 + \beta^2\right)
                            \cdot \left(\frac{precision(\ell) \cdot recall(\ell)}
                            {\beta^2 \cdot precision(\ell) + recall(\ell)}\right)$</td>
    </tr>
    <tr>
      <td>Weighted precision</td><td>$precision_{w}= \frac{1}{N} \sum\nolimits_{\ell \in L} precision(\ell) \cdot \left\|\mathbf{y}^{(\ell)}\right\|_0$</td>
    </tr>
    <tr>
      <td>Weighted recall</td><td>$recall_{w}= \frac{1}{N} \sum\nolimits_{\ell \in L} recall(\ell) \cdot \left\|\mathbf{y}^{(\ell)}\right\|_0$</td>
    </tr>
    <tr>
      <td>Weighted F-measure</td><td>$F_{w}(\beta)= \frac{1}{N} \sum\nolimits_{\ell \in L} F(\beta, \ell) \cdot \left\|\mathbf{y}^{(\ell)}\right\|_0$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippets illustrate how to load a sample dataset, train a
multiclass classification algorithm on the data, and evaluate the performance of
the algorithm by several multiclass classification evaluation metrics.

{% highlight scala %}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_multiclass_classification_data.txt")

// Split data into training (60%) and test (40%)
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

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
println(metrics.confusionMatrix)

// Overall Statistics
val precision = metrics.precision
val recall = metrics.recall // same as true positive rate
val f1Score = metrics.fMeasure
printf("""
Summary Statistics
Precision: %1.2f
Recall: %1.2f
F1 Score: %1.2f
""", precision, recall, f1Score)

// Precision by label
val labels = metrics.labels
labels.foreach(l => printf("Precision(%s): %1.2f\n", l, metrics.precision(l)))

// Recall by label
labels.foreach(l => printf("Recall(%s): %1.2f\n", l, metrics.recall(l)))

// False positive rate by label
labels.foreach(l => printf("FPR(%s): %1.2f\n", l, metrics.falsePositiveRate(l)))

// F-measure by label
labels.foreach(l => printf("F1 Score(%s): %1.2f\n", l, metrics.fMeasure(l)))

// Weighted stats
printf("Weighted precision: %1.2f\n", metrics.weightedPrecision)
printf("Weighted recall: %1.2f\n", metrics.weightedRecall)
printf("Weighted F1 score: %1.2f\n", metrics.weightedFMeasure)
printf("Weighted false positive rate: %1.2f\n", metrics.weightedFalsePositiveRate)

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
    for (int i = 0; i < metrics.labels().length - 1; i++) {
        System.out.format("Class %f precision = %f\n", metrics.labels()[i], metrics.precision(metrics.labels()[i]));
        System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(metrics.labels()[i]));
        System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(metrics.labels()[i]));
    }

    //Weighted stats
    System.out.format("Weighted precision = %1.2f\n", metrics.weightedPrecision());
    System.out.format("Weighted recall = %1.2f\n", metrics.weightedRecall());
    System.out.format("Weighted F1 score = %1.2f\n", metrics.weightedFMeasure());
    System.out.format("Weighted false positive rate = %1.2f\n", metrics.weightedFalsePositiveRate());

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
splits = data.randomSplit([0.6, 0.4], seed = 11L)
training = splits[0].cache()
test = splits[1]

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
print "Summary Stats"
print "Precision = %1.2f" % precision
print "Recall = %1.2f" % recall
print "F1 Score = %1.2f" % f1Score

# Statistics by class
labels = data.map(lambda lp: lp.label).distinct().collect()
for label in sorted(labels):
    print "Class %s precision = %1.2f" % (label, metrics.precision(label))
    print "Class %s recall = %1.2f" % (label, metrics.recall(label))
    print "Class %s F1 Measure = %1.2f" % (label, metrics.fMeasure(label, beta=1.0))

# Weighted stats
print "Weighted recall = %1.2f" % metrics.weightedRecall
print "Weighted precision = %1.2f" % metrics.weightedPrecision
print "Weighted F(1) Score = %1.2f" % metrics.weightedFMeasure()
print "Weighted F(0.5) Score = %1.2f" % metrics.weightedFMeasure(beta=0.5)
print "Weighted false positive rate = %1.2f" % metrics.weightedFalsePositiveRate
{% endhighlight %}

</div>
</div>

## Multilabel Classification

<div>
A multilabel classification problem involves mapping each document in a set of documents to a set of class labels. Here
we define a set $D$ of $N$ documents:
\[
    D = \left\{d_0, d_1, ..., d_{N-1}\right\}
\]
Define $\left\{L_0, L_1, ..., L_{N-1}\right\}$ to be a family of label sets and $\left\{P_0, P_1, ..., P_{N-1}\right\}$ to be a
family of prediction sets where $L_i$ and $P_i$ are the label set and prediction set, respectively, that correspond to document $d_i$.

The set of all unique labels is given by
\[
L = \bigcup_{k=0}^{N-1} L_k
\]
</div>


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
      <td>Precision by label</td><td>$precision(\ell)=\frac{TP}{TP + FP}=
          \frac{\sum_{i=0}^{N-1} (\ell \in P_i)\:\&\&\:(\ell \in L_i)}
          {\sum_{i=0}^{N-1} (\ell \in P_i)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td><td>$recall(\ell)=\frac{TP}{P}=
          \frac{\sum_{i=0}^{N-1} (\ell \in P_i)\:\&\&\:(\ell \in L_i)}
          {\sum_{i=0}^{N-1} (\ell \in L_i)}$</td>
    </tr>
    <tr>
      <td>F1-measure by label</td><td>$F(\beta, \ell) = 2
                            \cdot \left(\frac{precision(\ell) \cdot recall(\ell)}
                            {precision(\ell) + recall(\ell)}\right)$</td>
    </tr>
    <tr>
      <td>Hamming Loss</td>
      <td>
        $\frac{1}{N \cdot \left|L\right|} \sum_{i=0}^{N - 1} \left|L_i\right| + \left|P_i\right| - 2\left|L_i \cap P_i\right|$
      </td>
    </tr>
    <tr>
      <td>Subset Accuracy</td><td>$\frac{1}{N} \sum_{i=0}^{N-1} L_i=P_i$</td>
    </tr>
    <tr>
      <td>F1 Measure</td><td>$\frac{1}{N} \sum_{i=0}^{N-1} 2 \frac{\left|P_i \cap L_i\right|}{\left|P_i\right| \cdot \left|L_i\right|} $</td>
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
      <td>$2 \cdot \frac{TP}{2 \cdot TP + FP + FN}=2 \cdot \frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}{2 \cdot \sum_{i=0}^{N-1} \left|P_i \cap L_i\right|
      + \sum_{i=0}^{N-1} \left|L_i - P_i\right| + \sum_{i=0}^{N-1} \left|P_i - L_i\right|}$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippets illustrate how to evaluate the
performance of a multilabel classifer.

{% highlight scala %}
import org.apache.spark.mllib.evaluation.MultilabelMetrics

/**
 * Generate some fake prediction and label data for multilabel classification
 *
 * Documents true labels (5x class0, 3x class1, 4x class2):
 * doc 0 - predict 0, 1 - class 0, 2
 * doc 1 - predict 0, 2 - class 0, 1
 * doc 2 - predict none - class 0
 * doc 3 - predict 2 - class 2
 * doc 4 - predict 2, 0 - class 2, 0
 * doc 5 - predict 0, 1, 2 - class 0, 1
 * doc 6 - predict 1 - class 1, 2
 *
 * predicted classes
 * class 0 - doc 0, 1, 4, 5 (total 4)
 * class 1 - doc 0, 5, 6 (total 3)
 * class 2 - doc 1, 3, 4, 5 (total 4)
 *
 * true classes
 * class 0 - doc 0, 1, 2, 4, 5 (total 5)
 * class 1 - doc 1, 5, 6 (total 3)
 * class 2 - doc 0, 3, 4, 6 (total 4)
 *
 */
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
println("Recall = " + metrics.recall)
println("Precision = " + metrics.precision)
println("F1 measure = " + metrics.f1Measure)
println("Accuracy = " + metrics.accuracy)

// Individual label stats
metrics.labels.foreach(label => printf("Class %s precision = %1.2f\n", label, metrics.precision(label)))
metrics.labels.foreach(label => printf("Class %s recall = %1.2f\n", label, metrics.recall(label)))
metrics.labels.foreach(label => printf("Class %s F1-score = %1.2f\n", label, metrics.f1Measure(label)))

// Micro stats
println("Micro recall = " + metrics.microRecall)
println("Micro precision = " + metrics.microPrecision)
println("Micro F1 measure = " + metrics.microF1Measure)

// Hamming loss
println("Hamming loss = " + metrics.hammingLoss)

// Subset accuracy
println("Subset accuracy = " + metrics.subsetAccuracy)

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.evaluation.MultilabelMetrics;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext; // TODO: is this needed since using java sc?
import java.util.Arrays;
import java.util.List;

public class MultilabelClassification {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Multilabel Classification Metrics");
    JavaSparkContext sc = new JavaSparkContext(conf);
    /**
     * Generate some fake prediction and label data for multilabel classification
     *
     * Documents true labels (5x class0, 3x class1, 4x class2):
     * doc 0 - predict 0, 1 - class 0, 2
     * doc 1 - predict 0, 2 - class 0, 1
     * doc 2 - predict none - class 0
     * doc 3 - predict 2 - class 2
     * doc 4 - predict 2, 0 - class 2, 0
     * doc 5 - predict 0, 1, 2 - class 0, 1
     * doc 6 - predict 1 - class 1, 2
     *
     * predicted classes
     * class 0 - doc 0, 1, 4, 5 (total 4)
     * class 1 - doc 0, 5, 6 (total 3)
     * class 2 - doc 1, 3, 4, 5 (total 4)
     *
     * true classes
     * class 0 - doc 0, 1, 2, 4, 5 (total 5)
     * class 1 - doc 1, 5, 6 (total 3)
     * class 2 - doc 0, 3, 4, 6 (total 4)
     *
     */
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
    System.out.format("Recall = %1.2f\n", metrics.recall());
    System.out.format("Precision = %1.2f\n", metrics.precision());
    System.out.format("F1 measure = %1.2f\n", metrics.f1Measure());
    System.out.format("Accuracy = %1.2f\n", metrics.accuracy());

    // Stats by labels
    for (int i = 0; i < metrics.labels().length - 1; i++) {
        System.out.format("Class %1.1f precision = %1.2f\n", metrics.labels()[i], metrics.precision(metrics.labels()[i]));
        System.out.format("Class %1.1f recall = %1.2f\n", metrics.labels()[i], metrics.recall(metrics.labels()[i]));
        System.out.format("Class %1.1f F1 score = %1.2f\n", metrics.labels()[i], metrics.f1Measure(metrics.labels()[i]));
    }

    // Micro stats
    System.out.format("Micro recall = %1.2f\n", metrics.microRecall());
    System.out.format("Micro precision = %1.2f\n", metrics.microPrecision());
    System.out.format("Micro F1 measure = %1.2f\n", metrics.microF1Measure());

    // Hamming loss
    System.out.format("Hamming loss = %1.2f\n", metrics.hammingLoss());

    // Subset accuracy
    System.out.format("Subset accuracy = %1.2f\n", metrics.subsetAccuracy());

  }
}

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
from pyspark.mllib.evaluation import MultilabelMetrics

#
# Generate some fake prediction and label data for multilabel classification
#
# Documents true labels (5x class0, 3x class1, 4x class2):
# doc 0 - predict 0, 1 - class 0, 2
# doc 1 - predict 0, 2 - class 0, 1
# doc 2 - predict none - class 0
# doc 3 - predict 2 - class 2
# doc 4 - predict 2, 0 - class 2, 0
# doc 5 - predict 0, 1, 2 - class 0, 1
# doc 6 - predict 1 - class 1, 2
#
# predicted classes
# class 0 - doc 0, 1, 4, 5 (total 4)
# class 1 - doc 0, 5, 6 (total 3)
# class 2 - doc 1, 3, 4, 5 (total 4)
#
# true classes
# class 0 - doc 0, 1, 2, 4, 5 (total 5)
# class 1 - doc 1, 5, 6 (total 3)
# class 2 - doc 0, 3, 4, 6 (total 4)
#

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
print "Recall = %1.2f" % metrics.recall()
print "Precision = %1.2f" % metrics.precision()
print "F1 measure = %1.2f" % metrics.f1Measure()
print "Accuracy = %1.2f" % metrics.accuracy

# Individual label stats
labels = scoreAndLabels.flatMap(lambda x: x[1]).distinct().collect()
for label in labels:
    print "Class %s precision = %1.2f" % (label, metrics.precision(label))
    print "Class %s recall = %1.2f" % (label, metrics.recall(label))
    print "Class %s F1 Measure = %1.2f" % (label, metrics.f1Measure(label))

# Micro stats
print "Micro precision = %1.2f" % metrics.microPrecision
print "Micro recall = %1.2f" % metrics.microRecall
print "Micro F1 measure = %1.2f" % metrics.microF1Measure

# Hamming loss
print "Hamming loss = %1.2f" % metrics.hammingLoss

# Subset accuracy
print "Subset accuracy = %1.2f" % metrics.subsetAccuracy

{% endhighlight %}

</div>
</div>

## Ranking Systems

The role of a ranking algorithm (often thought of as a recommender system) is to return to the user a set of
relevant items or documents based on some training data. The definition of relevance may vary and is usually
application specific. Ranking system metrics aim to quantify the effectiveness of these rankings or recommendations
in various contexts. Some metrics compare a set of recommended documents to a ground truth set of relevant documents,
while other metrics may incorporate numerical ratings explicitly.

<div>
A ranking system usually deals with a set of $M$ users
\[
    U = \left\{u_0, u_1, ..., u_{M-1}\right\}
\]
Each user ($u_i$) having a set of $N$ ground truth relevant documents
\[
    D_i = \left\{d_0, d_1, ..., d_{N-1}\right\}
\]
And a list of $Q$ recommended documents, in order of decreasing relevance
\[
    R_i = \left[r_0, r_1, ..., r_{Q-1}\right]
\]

The goal of the ranking system is to produce the most relevant set
of documents for each user. The relevance of the sets and the
effectiveness of the algorithms can be measured using the metrics
listed below.
</div>

TODO: format NDCG cell
<table class="table">
  <thead>
    <tr><th>Metric</th><th>Description</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>
        Precision at k
      </td>
      <td>
        $precision(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{k} \sum_{j=0}^{k-1} rel(R_i(j), D_i)}$
      </td>
    </tr>
    <tr>
      <td>Mean Average Precision</td>
      <td>
        $MAP=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{\left|D_i\right|} \sum_{j=0}^{Q-1} \frac{rel(R_i(j), D_i)}{j + 1}}$
      </td>
    </tr>
    <tr>
      <td>Normalized Discounted Cumulative Gain</td>
      <td>
        $NDCG(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{IDCG(D_i, k)}\sum_{j=0}^{n-1} \frac{rel(R_i(j), D_i)}{log_2(i+1)}}\\
        n = min(max(|R_i|,|D_i|),k)\\
        rel(r, D) = \begin{cases}1 &amp; \text{if $r \in D$}, \\ 0 &amp;
                \text{otherwise}.\end{cases}\\
        IDCG(D, k) = \sum_{i=0}^{min(\left|D\right|, k) - 1} \frac{1}{log_2(i+1)}
        $
      </td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippets illustrate how to load a sample dataset, train
an alternating least squares recommendation model on the data, and evaluate
the performance of the recommender by several ranking metrics.

{% highlight scala %}
import org.apache.spark.mllib.evaluation.{RegressionMetrics, RankingMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

/**
 * MovieLens ratings are on a scale of 1-5:
 * 5: Must see
 * 4: Will enjoy
 * 3: It's okay
 * 2: Fairly bad
 * 1: Awful
 * So we should not recommend a movie if the predicted rating is less than 3.
 * To map ratings to confidence scores, we use
 * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
 * entries are generally between It's okay and Fairly bad.
 * The semantics of 0 in this expanded world of non-positive weights
 * are "the same as never having interacted at all".
 */
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
Array(1, 5, 10).foreach{ k =>
  printf("Precision at %d = %1.3f\n", k, metrics.precisionAt(k))
}

// Mean average precision
printf("Mean average precision = %1.3f\n", metrics.meanAveragePrecision)

// Normalized discounted cumulative gain
Array(1, 5, 10).foreach{ k =>
  printf("NDCG at %d = %1.3f\n", k, metrics.ndcgAt(k))
}

// Get predictions for each data point
val allPredictions = model.predict(ratings.map(r => (r.user, r.product))).map(r => ((r.user, r.product), r.rating))
val allRatings = ratings.map(r => ((r.user, r.product), r.rating))
val predictionsAndLabels = allPredictions.join(allRatings).map{ case ((user, product), (predicted, actual)) =>
  (predicted, actual)
}

// Get the RMSE using regression metrics
val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
printf("RMSE = %1.2f", regressionMetrics.rootMeanSquaredError)


{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
// TODO

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">

{% highlight python %}
# TODO

{% endhighlight %}

</div>
</div>

## Regression
Regression analysis, most commonly formulated as a least-squares minimization, involves
estimating the relationships among input and output variables. In this context, regression
metrics refer to a continuous output variable.

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Mean Squared Error (MSE)</td><td>$MSE = \frac{\sum_{i=0}^{N-1} (y_i - \hat{y}_i)^2}{N}$</td>
    </tr>
    <tr>
      <td>Root Mean Squared Error (RMSE)</td><td>$RMSE = \sqrt{\frac{\sum_{i=0}^{N-1} (y_i - \hat{y}_i)^2}{N}}$</td>
    </tr>
    <tr>
      <td>Coefficient of Determination $(R^2)$</td><td>$R^2=1 - \frac{MSE}{VAR(\boldsymbol{y}) \cdot (N-1)}=
        1-\frac{\sum_{i=0}^{N-1} (y_i - \hat{y}_i)^2}{\sum_{i=0}^{N-1}(y_i-\bar{y})^2}$</td>
    </tr>
    <tr>
      <td>Mean Absoloute Error (MAE)</td><td>$MAE=\sum_{i=0}^{N-1} \left|y_i - \hat{y}_i\right|$</td>
    </tr>
    <tr>
      <td>Explained Variance</td><td>$1 - \frac{VAR(\boldsymbol{y} - \boldsymbol{\hat{y}})}{VAR(\boldsymbol{y})}$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippets illustrate how to load a sample dataset, train a
linear regression algorithm on the data, and evaluate the performance of
the algorithm by several regression metrics.

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
println("MSE = " + metrics.meanSquaredError)
println("RMSE = " + metrics.rootMeanSquaredError)

// R-squared
println("R-squared = " + metrics.r2)

// Mean absolute error
println("MAE = " + metrics.meanAbsoluteError)

// Explained variance
println("Explained variance = " + metrics.explainedVariance)


{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import scala.Tuple2;
//TODO: clean up imports
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
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
    System.out.println("MSE = " + metrics.meanSquaredError());
    System.out.println("RMSE = " + metrics.rootMeanSquaredError());

    // R-squared
    System.out.println("R Squared = " + metrics.r2());

    // Mean absolute error
    System.out.println("MAE = " + metrics.meanAbsoluteError());

    // Explained variance
    System.out.println("Explained Variance = " + metrics.explainedVariance());

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

# Load and parse the data
def parsePoint(line):
    values = line.split()
    return LabeledPoint(float(values[0]), DenseVector([float(x.split(':')[1]) for x in values[1:]]))

data = sc.textFile("data/mllib/sample_linear_regression_data.txt")
parsedData = data.map(parsePoint)

# Get predictions
valuesAndPreds = parsedData.map(lambda p: (float(model.predict(p.features)), p.label))

# Instantiate metrics object
metrics = RegressionMetrics(valuesAndPreds)

# Squared Error
print "MSE = %1.2f" % metrics.meanSquaredError
print "RMSE = %1.2f" % metrics.rootMeanSquaredError

# R-squared
print "R-squared = %1.2f" % metrics.r2

# Mean absolute error
print "MAE = %1.2f" % metrics.meanAbsoluteError

# Explained variance
print "Explained variance = %1.2f" % metrics.explainedVariance

{% endhighlight %}

</div>
</div>