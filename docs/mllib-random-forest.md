---
layout: global
title: Random Forest - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Random Forest
---

* Table of contents
{:toc}

[Random forests](http://en.wikipedia.org/wiki/Random_forest)
are ensembles of [decision trees](mllib-decision-tree.html).
Random forests are one of the most successful machine learning models for classification and
regression.  They combine many decision trees in order to reduce the risk of overfitting.
Like decision trees, random forests handle categorical features,
extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions.

MLlib supports random forests for binary and multiclass classification and for regression,
using both continuous and categorical features.
MLlib implements random forests using the existing [decision tree](mllib-decision-tree.html)
implementation.  Please see the decision tree guide for more information on trees.

MLlib also supports learning ensembles of trees using Gradient Boosted Trees.  Refer to the [Gradient Boosted Trees guide](mllib-gbt.html) for a comparison of the two methods.

## Basic algorithm

Random forests train a set of decision trees separately, so the training can be done in parallel.
The algorithm injects randomness into the training process so that each decision tree is a bit
different.  Combining the predictions from each tree reduces the variance of the predictions,
improving the performance on test data.

### Training

The randomness injected into the training process includes:

* Subsampling the original dataset on each iteration to get a different training set (a.k.a. bootstrapping).
* Considering different random subsets of features to split on at each tree node.

Apart from these randomizations, decision tree training is done in the same way as for individual decision trees.

### Prediction

To make a prediction on a new instance, a random forest must aggregate the predictions from its set of decision trees.  This aggregation is done differently for classification and regression.

*Classification*: Majority vote. Each tree's prediction is counted as a vote for one class.  The label is predicted to be the class which receives the most votes.

*Regression*: Averaging. Each tree predicts a real value.  The label is predicted to be the average of the tree predictions.

## Usage guide

We include a few guidelines for using random forests by discussing the various parameters.
We omit some decision tree parameters since those are covered in the [decision tree guide](mllib-decision-tree.html).

The first two parameters we mention are the most important, and tuning them can often improve performance:

* **numTrees**: Number of trees in the forest.
  * Increasing the number of trees will decrease the variance in predictions, improving the model's test-time accuracy.
  * Training time increases roughly linearly in the number of trees.

* **maxDepth**: Maximum depth of each tree in the forest.
  * Increasing the depth makes the model more expressive and powerful.  However, deep trees take longer to train and are also more prone to overfitting.
  * In general, it is acceptable to train deeper trees when using random forests than when using a single decision tree.  One tree is more likely to overfit than a random forest (because of the variance reduction from averaging multiple trees in the forest).

The next two parameters generally do not require tuning.  However, they can be tuned to speed up training.

* **subsamplingRate**: This parameter specifies the size of the dataset used for training each tree in the forest, as a fraction of the size of the original dataset.  The default (1.0) is recommended, but decreasing this fraction can speed up training.

* **featureSubsetStrategy**: Number of features to use as candidates for splitting at each tree node.  The number is specified as a fraction or function of the total number of features.  Decreasing this number will speed up training, but can sometimes impact performance if too low.

## Examples

### Classification

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform classification using a Random Forest.
The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import java.util.HashMap;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Load and parse the data file.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();
// Split the data into training and test sets (30% held out for testing)
JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
JavaRDD<LabeledPoint> trainingData = splits[0];
JavaRDD<LabeledPoint> testData = splits[1];

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
Integer numClasses = 2;
Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
String impurity = "gini";
Integer maxDepth = 5;
Integer maxBins = 32;

// Train a DecisionTree model for classification.
final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on test instances and compute test error
JavaPairRDD<Double, Double> predictionAndLabel =
  testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override
    public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
Double testErr =
  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
    @Override
    public Boolean call(Tuple2<Double, Double> pl) {
      return !pl._1().equals(pl._2());
    }
  }).count() / testData.count();
System.out.println("Test Error: " + testErr);
System.out.println("Learned classification tree model:\n" + model.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load and parse the data file into an RDD of LabeledPoint.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))
print('Learned classification tree model:')
print(model.toDebugString())
{% endhighlight %}

Note: When making predictions for a dataset, it is more efficient to do batch prediction rather
than separately calling `predict` on each data point.  This is because the Python code makes calls
to an underlying `DecisionTree` model in Scala.
</div>

</div>

### Regression

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform regression using a Random Forest.
The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "variance"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelsAndPredictions = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Test Mean Squared Error = " + testMSE)
println("Learned regression tree model:\n" + model.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import java.util.HashMap;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;

SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Load and parse the data file.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();
// Split the data into training and test sets (30% held out for testing)
JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
JavaRDD<LabeledPoint> trainingData = splits[0];
JavaRDD<LabeledPoint> testData = splits[1];

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
String impurity = "variance";
Integer maxDepth = 5;
Integer maxBins = 32;

// Train a DecisionTree model.
final DecisionTreeModel model = DecisionTree.trainRegressor(trainingData,
  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on test instances and compute test error
JavaPairRDD<Double, Double> predictionAndLabel =
  testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override
    public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
Double testMSE =
  predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
    @Override
    public Double call(Tuple2<Double, Double> pl) {
      Double diff = pl._1() - pl._2();
      return diff * diff;
    }
  }).reduce(new Function2<Double, Double, Double>() {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }) / data.count();
System.out.println("Test Mean Squared Error: " + testMSE);
System.out.println("Learned regression tree model:\n" + model.toDebugString());
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load and parse the data file into an RDD of LabeledPoint.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(testData.count())
print('Test Mean Squared Error = ' + str(testMSE))
print('Learned regression tree model:')
print(model.toDebugString())
{% endhighlight %}

Note: When making predictions for a dataset, it is more efficient to do batch prediction rather
than separately calling `predict` on each data point.  This is because the Python code makes calls
to an underlying `DecisionTree` model in Scala.
</div>

</div>
