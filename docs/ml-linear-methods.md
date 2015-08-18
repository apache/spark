---
layout: global
title: Linear Methods - ML
displayTitle: <a href="ml-guide.html">ML</a> - Linear Methods
---


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


In MLlib, we implement popular linear methods such as logistic regression and linear least squares with L1 or L2 regularization. Refer to [the linear methods in mllib](mllib-linear-methods.html) for details. In `spark.ml`, we also include Pipelines API for [Elastic net](http://en.wikipedia.org/wiki/Elastic_net_regularization), a hybrid of L1 and L2 regularization proposed in [this paper](http://users.stat.umn.edu/~zouxx019/Papers/elasticnet.pdf). Mathematically it is defined as a linear combination of the L1-norm and the L2-norm:
`\[
\alpha \|\wv\|_1 + (1-\alpha) \frac{1}{2}\|\wv\|_2^2, \alpha \in [0, 1].
\]`
By setting $\alpha$ properly, it contains both L1 and L2 regularization as special cases. For example, if a [linear regression](https://en.wikipedia.org/wiki/Linear_regression) model is trained with the elastic net parameter $\alpha$ set to $1$, it is equivalent to a [Lasso](http://en.wikipedia.org/wiki/Least_squares#Lasso_method) model. On the other hand, if $\alpha$ is set to $0$, the trained model reduces to a [ridge regression](http://en.wikipedia.org/wiki/Tikhonov_regularization) model. We implement Pipelines API for both linear regression and logistic regression with elastic net regularization.

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">

{% highlight scala %}

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.util.MLUtils

// Load training data
val training = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(training)

// Print the weights and intercept for logistic regression
println(s"Weights: ${lrModel.weights} Intercept: ${lrModel.intercept}")

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class LogisticRegressionWithElasticNetExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
      .setAppName("Logistic Regression with Elastic Net Example");

    SparkContext sc = new SparkContext(conf);
    SQLContext sql = new SQLContext(sc);
    String path = "sample_libsvm_data.txt";

    // Load training data
    DataFrame training = sql.createDataFrame(MLUtils.loadLibSVMFile(sc, path).toJavaRDD(), LabeledPoint.class);

    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    LogisticRegressionModel lrModel = lr.fit(training);

    // Print the weights and intercept for logistic regression
    System.out.println("Weights: " + lrModel.weights() + " Intercept: " + lrModel.intercept());
  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

{% highlight python %}

from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

# Load training data
training = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(training)

# Print the weights and intercept for logistic regression
print("Weights: " + str(lrModel.weights))
print("Intercept: " + str(lrModel.intercept))
{% endhighlight %}

</div>

</div>

### Optimization

The optimization algorithm underlies the implementation is called [Orthant-Wise Limited-memory QuasiNewton](http://research-srv.microsoft.com/en-us/um/people/jfgao/paper/icml07scalable.pdf)
(OWL-QN). It is an extension of L-BFGS that can effectively handle L1 regularization and elastic net.

### Model Summaries

Once a linear model is fit on data, it is useful to extract statistics such as the
loss per iteration and metrics to understand how well the model has performed on training
and test data. The examples provided below will help in understanding how to use the summaries
obtained by the summary method of the fitted linear models.

Note that the predictions and metrics which are stored as dataframes obtained from the summary
are transient and are not available on the driver. This is because these are as expensive
to store as the original data itself.

#### Example: Summary for LogisticRegression
<div class="codetabs">
<div data-lang="scala">
[`LogisticRegressionTrainingSummary`](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegressionTrainingSummary)
provides an interface to access information such as `objectiveHistory` and metrics
to evaluate the performance on the training data directly with very less code to be rewritten by
the user. [`LogisticRegression`](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegression)
currently supports only binary classification and hence in order to access the binary metrics
the summary must be explicitly cast to
[BinaryLogisticRegressionTrainingSummary](api/scala/index.html#org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary)
as done in the code below. This avoids raising errors for multiclass outputs while providing
extensiblity when multiclass classification is supported in the future.

This example illustrates the use of `LogisticRegressionTrainingSummary` on some toy data.

{% highlight scala %}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{LogisticRegression, BinaryLogisticRegressionSummary}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}

val conf = new SparkConf().setAppName("LogisticRegressionSummary")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

// Use some random data for demonstration.
// Note that the RDD of LabeledPoints can be converted to a dataframe directly.
val data = sc.parallelize(Array(
  LabeledPoint(0.0, Vectors.dense(0.2, 4.5, 1.6)),
  LabeledPoint(1.0, Vectors.dense(3.1, 6.8, 3.6)),
  LabeledPoint(0.0, Vectors.dense(2.4, 0.9, 1.9)),
  LabeledPoint(1.0, Vectors.dense(9.1, 3.1, 3.6)),
  LabeledPoint(0.0, Vectors.dense(2.5, 1.9, 9.1)))
)
val logRegDataFrame = data.toDF()

// Run Logistic Regression on your toy data.
// Since LogisticRegression is an estimator, it returns an instance of LogisticRegressionModel
// which is a transformer.
val logReg = new LogisticRegression().setMaxIter(5).setRegParam(0.01)
val logRegModel = logReg.fit(logRegDataFrame)

// Extract the summary directly from the returned LogisticRegressionModel instance.
val trainingSummary = logRegModel.summary

// Obtain the loss per iteration.
val objectiveHistory = trainingSummary.objectiveHistory
objectiveHistory.foreach(loss => println(loss))

// Obtain the metrics useful to judge performance on test data.
// We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
// binary classification problem.
val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

// Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
val roc = binarySummary.roc
roc.show()
roc.select("FPR").show()
println(binarySummary.areaUnderROC)

// Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
// this selected threshold.
val fMeasure = binarySummary.fMeasureByThreshold
val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).
  select("threshold").head().getDouble(0)
logReg.setThreshold(bestThreshold)
logReg.fit(logRegDataFrame)
{% endhighlight %}
</div>

<div data-lang="java">
[`LogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/LogisticRegressionTrainingSummary)
provides an interface to access information such as `objectiveHistory` and metrics
to evaluate the performance on the training data directly with very less code to be rewritten by
the user. [`LogisticRegression`](api/java/org/apache/spark/ml/classification/LogisticRegression)
currently supports only binary classification and hence in order to access the binary metrics
the summary must be explicitly cast to
[BinaryLogisticRegressionTrainingSummary](api/java/org/apache/spark/ml/classification/LogisticRegressionTrainingSummary)
as done in the code below. This avoids raising errors for multiclass outputs while providing
extensiblity when multiclass classification is supported in the future

This example illustrates the use of `LogisticRegressionTrainingSummary` on some toy data.

{% highlight java %}
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.sql.functions.*;

SparkConf conf = new SparkConf().setAppName("LogisticRegressionSummary");
JavaSparkContext jsc = new JavaSparkContext(conf);
SQLContext jsql = new SQLContext(jsc);

// Use some random data for demonstration.
// Note that the RDD of LabeledPoints can be converted to a dataframe directly.
JavaRDD<LabeledPoint> data = sc.parallelize(Lists.newArrayList(
  new LabeledPoint(0.0, Vectors.dense(0.2, 4.5, 1.6)),
  new LabeledPoint(1.0, Vectors.dense(3.1, 6.8, 3.6)),
  new LabeledPoint(0.0, Vectors.dense(2.4, 0.9, 1.9)),
  new LabeledPoint(1.0, Vectors.dense(9.1, 3.1, 3.6)),
  new LabeledPoint(0.0, Vectors.dense(2.5, 1.9, 9.1)))
);
DataFrame logRegDataFrame = sql.createDataFrame(data, LabeledPoint.class);

// Run Logistic Regression on your toy data.
// Since LogisticRegression is an estimator, it returns an instance of LogisticRegressionModel
// which is a transformer.
LogisticRegression logReg = new LogisticRegression().setMaxIter(5).setRegParam(0.01);
LogisticRegressionModel logRegModel = logReg.fit(logRegDataFrame);

// Extract the summary directly from the returned LogisticRegressionModel instance.
LogisticRegressionTrainingSummary trainingSummary = logRegModel.summary();

// Obtain the loss per iteration.
double[] objectiveHistory = trainingSummary.objectiveHistory();
for (double lossPerIteration: objectiveHistory) {
  System.out.println(lossPerIteration);
}

// Obtain the metrics useful to judge performance on test data.
BinaryLogisticRegressionSummary binarySummary = (BinaryLogisticRegressionSummary) trainingSummary;

// Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
DataFrame roc = binarySummary.roc();
roc.show();
roc.select("FPR").show();
System.out.println(binarySummary.areaUnderROC());

// Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
// this selected threshold.
DataFrame fMeasure = binarySummary.fMeasureByThreshold();
double maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0);
double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure)).
  select("threshold").head().getDouble(0);
logReg.setThreshold(bestThreshold);
logReg.fit(logRegDataFrame);
{% endhighlight %}
</div>
</div>
