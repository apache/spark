---
layout: global
title: Gradient-Boosted Trees - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Gradient-Boosted Trees
---

* Table of contents
{:toc}

[Gradient-Boosted Trees (GBTs)](http://en.wikipedia.org/wiki/Gradient_boosting)
are ensembles of [decision trees](mllib-decision-tree.html).
GBTs iteratively train decision trees in order to minimize a loss function.
Like decision trees, GBTs handle categorical features,
extend to the multiclass classification setting, do not require
feature scaling, and are able to capture non-linearities and feature interactions.

MLlib supports GBTs for binary classification and for regression,
using both continuous and categorical features.
MLlib implements GBTs using the existing [decision tree](mllib-decision-tree.html) implementation.  Please see the decision tree guide for more information on trees.

*Note*: GBTs do not yet support multiclass classification.  For multiclass problems, please use
[decision trees](mllib-decision-tree.html) or [Random Forests](mllib-random-forest.html).

## Basic algorithm

Gradient boosting iteratively trains a sequence of decision trees.
On each iteration, the algorithm uses the current ensemble to predict the label of each training instance and then compares the prediction with the true label.  The dataset is re-labeled to put more weight on training instances with poor predictions.  Thus, in the next iteration, the decision tree will help correct for previous mistakes.

The specific weight mechanism is defined by a loss function (discussed below).  With each iteration, GBTs further reduce this loss function on the training data.

### Comparison with Random Forests

Both GBTs and [Random Forests](mllib-random-forest.html) are algorithms for learning ensembles of trees, but the training processes are different.  There are several practical trade-offs:

 * GBTs may be able to achieve the same accuracy using fewer trees, so the model produced may be smaller (faster for test time prediction).
 * GBTs train one tree at a time, so they can take longer to train than random forests.  Random Forests can train multiple trees in parallel.
   * On the other hand, it is often reasonable to use smaller trees with GBTs than with Random Forests, and training smaller trees takes less time.
 * Random Forests can be less prone to overfitting.  Training more trees in a Random Forest reduces the likelihood of overfitting, but training more trees with GBTs increases the likelihood of overfitting.

In short, both algorithms can be effective.  GBTs may be more useful if test time prediction speed is important.  Random Forests are arguably more successful in industry.

### Losses

The table below lists the losses currently supported by GBTs in MLlib.
Note that each loss is applicable to one of classification or regression, not both.

Notation: $N$ = number of instances. $y_i$ = label of instance $i$.  $x_i$ = features of instance $i$.  $F(x_i)$ = model's predicted label for instance $i$.

<table class="table">
  <thead>
    <tr><th>Loss</th><th>Task</th><th>Formula</th><th>Description</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Log Loss</td>
	  <td>Classification</td>
	  <td>$2 \sum_{i=1}^{N} \log(1+\exp(-2 y_i F(x_i)))$</td><td>Twice binomial negative log likelihood.</td>
    </tr>
    <tr>
      <td>Squared Error</td>
	  <td>Regression</td>
	  <td>$\sum_{i=1}^{N} \frac{1}{2} (y_i - F(x_i))^2$</td><td>Also called L2 loss.  Default loss for regression tasks.</td>
    </tr>
    <tr>
      <td>Absolute Error</td>
	  <td>Regression</td>
     <td>$\sum_{i=1}^{N} |y_i - F(x_i)|$</td><td>Also called L1 loss.  Can be more robust to outliers than Squared Error.</td>
    </tr>
  </tbody>
</table>

## Usage guide

We include a few guidelines for using GBTs by discussing the various parameters.
We omit some decision tree parameters since those are covered in the [decision tree guide](mllib-decision-tree.html).

* **loss**: See the section above for information on losses and their applicability to tasks (classification vs. regression).  Different losses can give significantly different results, depending on the dataset.

* **numIterations**: This sets the number of trees in the ensemble.  Each iteration produces one tree.  Increasing this number makes the model more expressive, improving training data accuracy.  However, test-time accuracy may suffer if this is too large.

* **learningRate**: This parameter should not need to be tuned.  If the algorithm behavior seems unstable, decreasing this value may improve stability.

* **algo**: The algorithm or task (classification vs. regression) is set using the tree [Strategy] parameter.


## Examples

### Classification

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform classification using Gradient-Boosted Trees with log loss.
The test error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a GradientBoostedTrees model.
//  The defaultParams for Classification use LogLoss by default.
val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
boostingStrategy.treeStrategy.numClassesForClassification = 2
boostingStrategy.treeStrategy.maxDepth = 5
//  Empty categoricalFeaturesInfo indicates all features are continuous.
boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification GBT model:\n" + model.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

SparkConf sparkConf = new SparkConf().setAppName("JavaGradientBoostedTrees");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Load and parse the data file.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();
// Split the data into training and test sets (30% held out for testing)
JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
JavaRDD<LabeledPoint> trainingData = splits[0];
JavaRDD<LabeledPoint> testData = splits[1];

// Train a GradientBoostedTrees model.
//  The defaultParams for Classification use LogLoss by default.
BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
boostingStrategy.getTreeStrategy().setNumClassesForClassification(2);
boostingStrategy.getTreeStrategy().setMaxDepth(5);
//  Empty categoricalFeaturesInfo indicates all features are continuous.
Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

final GradientBoostedTreesModel model =
  GradientBoostedTrees.train(trainingData, boostingStrategy);

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
System.out.println("Learned classification GBT model:\n" + model.toDebugString());
{% endhighlight %}
</div>

</div>

### Regression

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform regression using Gradient-Boosted Trees with Squared Error as the loss.
The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a GradientBoostedTrees model.
//  The defaultParams for Regression use SquaredError by default.
val boostingStrategy = BoostingStrategy.defaultParams("Regression")
boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
boostingStrategy.treeStrategy.maxDepth = 5
//  Empty categoricalFeaturesInfo indicates all features are continuous.
boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

// Evaluate model on test instances and compute test error
val labelsAndPredictions = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Test Mean Squared Error = " + testMSE)
println("Learned regression GBT model:\n" + model.toDebugString)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

SparkConf sparkConf = new SparkConf().setAppName("JavaGradientBoostedTrees");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Load and parse the data file.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();
// Split the data into training and test sets (30% held out for testing)
JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
JavaRDD<LabeledPoint> trainingData = splits[0];
JavaRDD<LabeledPoint> testData = splits[1];

// Train a GradientBoostedTrees model.
//  The defaultParams for Regression use SquaredError by default.
BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
boostingStrategy.getTreeStrategy().setMaxDepth(5);
//  Empty categoricalFeaturesInfo indicates all features are continuous.
Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

final GradientBoostedTreesModel model =
  GradientBoostedTrees.train(trainingData, boostingStrategy);

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
System.out.println("Learned regression GBT model:\n" + model.toDebugString());
{% endhighlight %}
</div>

</div>
