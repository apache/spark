---
layout: global
title: Linear Methods - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Linear Methods
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
      <td>F-measure</td><td>$F(\beta) := \left(1 + \beta^2\right)
                            \cdot \left(\frac{precision \cdot recall}
                            {\left(\beta^2 \cdot precision\right) + recall}\right)$</td>
    </tr>
    <tr>
      <td>Receiver Operating Characteristic</td><td>$FPR(T)=\int^\infty_{T} P_0(T)\,dT\\
                                                    TPR(T)=\int^\infty_{T} P_1(T)\,dT$</td>
    </tr>
    <tr>
      <td>Area Under ROC Curve</td><td>$AUC=\int^\infty_{-\infty} TPR(T)FPR'(T)\,dT$</td>
    </tr>
    <tr>
      <td>Area Under Precision-Recall Curve</td><td>$AUPRC=\int^\infty_{-\infty} P(T)R(T)\,dT$</td>
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
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(10)
  .run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object.
val metrics = new BinaryClassificationMetrics(predictionAndLabels)

// Precision by threshold
val precision = metrics.precisionByThreshold
precision.foreach(x => printf("Threshold: %1.2f, Precision: %1.2f\n", x._1, x._2))

// Recall by threshold
val recall = metrics.precisionByThreshold
recall.foreach(x => printf("Threshold: %1.2f, Recall: %1.2f\n", x._1, x._2))

// Compute thresholds
// TODO: val thresholds = predictionAndLabels.sortByKey(false).map(_._1)

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

// ROC Curve
val roc = metrics.roc

// AUROC
val auROC = metrics.areaUnderROC
println("Area under ROC = " + auROC)

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


## Multiclass

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Confusion Matrix</td><td>TODO</td>
    </tr>
    <tr>
      <td>Overall Precision</td><td>$precision=\frac{TP}{TP + FP}=
          \frac{1}{N}\sum_{i=0}^{N} predicted_{i}==actual_{i}$</td>
    </tr>
    <tr>
      <td>Overall Recall</td><td>$recall=\frac{TP}{TP + FN}=
          \frac{1}{N}\sum_{i=0}^{N} predicted_{i}==actual_{i}$</td>
    </tr>
    <tr>
      <td>Overall F-measure</td><td>$F(\beta) := \left(1 + \beta^2\right)
                            \cdot \left(\frac{precision \cdot recall}
                            {\left(\beta^2 \cdot precision\right) + recall}\right)$</td>
    </tr>
    <tr>
      <td>Precision by label</td><td>$precision(label)=\frac{TP}{TP + FP}=
          \frac{\sum_{i=0}^{N} (predicted_{i}==label)\:\&\&\:(actual_{i}==label)}
          {\sum_{i=0}^{N} (predicted_{i}==label)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td><td>$recall(label)=\frac{TP}{P}=
          \frac{\sum_{i=0}^{N} (predicted_{i}==label)\:\&\&\:(actual_{i}==label)}
          {\sum_{i=0}^{N} (actual_{i}==label)}$</td>
    </tr>
    <tr>
      <td>F-measure by label</td><td>$F(\beta, label) := \left(1 + \beta^2\right)
                            \cdot \left(\frac{precision(label) \cdot recall(label)}
                            {\left(\beta^2 \cdot precision(label)\right) + recall(label)}\right)$</td>
    </tr>
    <tr>
      <td>Weighted precision</td><td>$recall_{w}= \frac{1}{N} \sum_{i=0}^{L} precision(label_i) \cdot count(label_i)$</td>
    </tr>
    <tr>
      <td>Weighted recall</td><td>$recall_{w}= \frac{1}{N} \sum_{i=0}^{L} recall(label_i) \cdot count(label_i)$</td>
    </tr>
    <tr>
      <td>Weighted F-measure</td><td>$F_{w}(\beta)= \frac{1}{N} \sum_{i=0}^{L} F(\beta, label_i) \cdot count(label_i)$</td>
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
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_multiclass_classification_data.txt")

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(3)
  .run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object.
val metrics = new MulticlassMetrics(predictionAndLabels)

// Confusion matrix
val confusionMatrix = metrics.confusionMatrix

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
printf("Weighted precision: %1.2f\n", metrics.weightedRecall)
printf("Weighted recall: %1.2f\n", metrics.weightedRecall)
printf("Weighted F1 score: %1.2f\n", metrics.weightedFMeasure)
printf("Weighted false positive rate: %1.2f\n", metrics.weightedFalsePositiveRate)

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

## Multilabel

## Ranking

### Ranking systems
The role of a ranking algorithm (often thought of as a recommender system) is to return to the user a set of
relevant items or documents based on some training data. The definition of relevance may vary and is usually
application specific. Ranking system metrics aim to quantify the effectiveness of these rankings or recommendations
in various contexts. Some metrics compare a set of recommended documents to a ground truth set of relevant documents,
while other metrics may incorporate numerical ratings explicitly.

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
        Precision at k is used to compare a set of recommended documents against a set of ground truth relevant
        documents. For each user, the fraction of the top k recommended documents that are in the set of relevant
        documents is computed. This result is then averaged across all users to produce the precision at k metric. It
        is important to note that the order of the recommended documents does not matter in this metric.
      </td>
    </tr>
    <tr>
      <td>Mean Average Precision</td>
      <td>
        Mean average precision is a measure of precision averaged across all users where order _does_ matter. For each user, a
        precision score is calculated by counting the number of recommended documents that are in the set of relevant
        documents, where highly ranked documents are given more weight. The mean average precision is the average of the
        precision scores for each user.
      </td>
    </tr>
    <tr>
      <td>Normalized Discounted Cumulative Gain</td>
      <td>
        $\frac{1}{IDCG_k}\sum_{i=1}^{N} \frac{rel_i}{log_2(i+1)}\\
        N = \min{\max{size(\hat{\boldsymbol{d}})}{size(\boldsymbol{d}) }}{k}\\
        rel_i = \begin{cases}1 &amp; \text{if $\hat{d}_i \in \boldsymbol{d}$}, \\ 0 &amp;
                \text{otherwise}.\end{cases}\\
        IDCG_k = \sum_{i=1}^{N} \frac{1}{log_2(i+1)}
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
// TODO: Explain what is going on in comments a bit better
import org.apache.spark.mllib.evaluation.{RegressionMetrics, RankingMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

// Load the data
val ratings = sc.textFile("sample_movielens_data.txt").map { line =>
  val fields = line.split("::")
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
}.cache()

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

// Get recommendations
def scaledRating(r: Rating): Rating = {
  val scaledRating = math.max(math.min(r.rating, 1.0), 0.0)
  Rating(r.user, r.product, scaledRating)
}
val userRecommended = model.recommendProductsForUsers(10).map{ case (user, recs) =>
  (user, recs.map(scaledRating))
}
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

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Mean Squared Error (MSE)</td><td>$MSE = \frac{\sum_{i=0}^N (y_i - \hat{y}_i)^2}{N}$</td>
    </tr>
    <tr>
      <td>Root Mean Squared Error (RMSE)</td><td>$RMSE = \sqrt{\frac{\sum_{i=0}^N (y_i - \hat{y}_i)^2}{N}}$</td>
    </tr>
    <tr>
      <td>Coefficient of Determination $(R^2)$</td><td>$R^2=1 - \frac{MSE}{VAR(\boldsymbol{y}) \cdot (N-1)}=
        1-\sum_{i=0}^N \frac{(y_i - \hat{y}_i)^2}{(y_i-\bar{y})^2}$</td>
    </tr>
    <tr>
      <td>Mean Absoloute Error (MAE)</td><td>$MAE=\sum_{i=0}^N \left|y_i - \hat{y}_i\right|$</td>
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
binary classification algorithm on the data, and evaluate the performance of
the algorithm by several binary evaluation metrics.

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
// TODO

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

data = sc.textFile("spark-1.4.0/data/mllib/sample_linear_regression_data.txt")
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