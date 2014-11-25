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

TODO

### Classification

The example below demonstrates how to load a
[LIBSVM data file](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/),
parse it as an RDD of `LabeledPoint` and then
perform classification using a decision tree with Gini impurity as an impurity measure and a
maximum tree depth of 5. The training error is calculated to measure the algorithm accuracy.

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").cache()

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)

// Evaluate model on training instances and compute training error
val labelAndPreds = data.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / data.count
println("Training Error = " + trainErr)
println("Learned classification tree model:\n" + model)
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
// Cache the data since we will use it again to compute training error.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
Integer numClasses = 2;
HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
String impurity = "gini";
Integer maxDepth = 5;
Integer maxBins = 32;

// Train a DecisionTree model for classification.
final DecisionTreeModel model = DecisionTree.trainClassifier(data, numClasses,
  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on training instances and compute training error
JavaPairRDD<Double, Double> predictionAndLabel =
  data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
Double trainErr =
  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
    @Override public Boolean call(Tuple2<Double, Double> pl) {
      return !pl._1().equals(pl._2());
    }
  }).count() / data.count();
System.out.println("Training error: " + trainErr);
System.out.println("Learned classification tree model:\n" + model);
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load and parse the data file into an RDD of LabeledPoint.
# Cache the data since we will use it again to compute training error.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt').cache()

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainClassifier(data, numClasses=2, categoricalFeaturesInfo={},
                                     impurity='gini', maxDepth=5, maxBins=32)

# Evaluate model on training instances and compute training error
predictions = model.predict(data.map(lambda x: x.features))
labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
trainErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(data.count())
print('Training Error = ' + str(trainErr))
print('Learned classification tree model:')
print(model)
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
perform regression using a decision tree with variance as an impurity measure and a maximum tree
depth of 5. The Mean Squared Error (MSE) is computed at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

<div class="codetabs">

<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").cache()

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "variance"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo, impurity,
  maxDepth, maxBins)

// Evaluate model on training instances and compute training error
val labelsAndPredictions = data.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val trainMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
println("Training Mean Squared Error = " + trainMSE)
println("Learned regression tree model:\n" + model)
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

// Load and parse the data file.
// Cache the data since we will use it again to compute training error.
String datapath = "data/mllib/sample_libsvm_data.txt";
JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();

SparkConf sparkConf = new SparkConf().setAppName("JavaDecisionTree");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
String impurity = "variance";
Integer maxDepth = 5;
Integer maxBins = 32;

// Train a DecisionTree model.
final DecisionTreeModel model = DecisionTree.trainRegressor(data,
  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Evaluate model on training instances and compute training error
JavaPairRDD<Double, Double> predictionAndLabel =
  data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
    @Override public Tuple2<Double, Double> call(LabeledPoint p) {
      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
    }
  });
Double trainMSE =
  predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
    @Override public Double call(Tuple2<Double, Double> pl) {
      Double diff = pl._1() - pl._2();
      return diff * diff;
    }
  }).reduce(new Function2<Double, Double, Double>() {
    @Override public Double call(Double a, Double b) {
      return a + b;
    }
  }) / data.count();
System.out.println("Training Mean Squared Error: " + trainMSE);
System.out.println("Learned regression tree model:\n" + model);
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# Load and parse the data file into an RDD of LabeledPoint.
# Cache the data since we will use it again to compute training error.
data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt').cache()

# Train a DecisionTree model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=32)

# Evaluate model on training instances and compute training error
predictions = model.predict(data.map(lambda x: x.features))
labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
trainMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(data.count())
print('Training Mean Squared Error = ' + str(trainMSE))
print('Learned regression tree model:')
print(model)
{% endhighlight %}

Note: When making predictions for a dataset, it is more efficient to do batch prediction rather
than separately calling `predict` on each data point.  This is because the Python code makes calls
to an underlying `DecisionTree` model in Scala.
</div>

</div>
