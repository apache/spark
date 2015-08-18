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


In MLlib, we implement popular linear methods such as logistic
regression and linear least squares with $L_1$ or $L_2$ regularization.
Refer to [the linear methods in mllib](mllib-linear-methods.html) for
details.  In `spark.ml`, we also include Pipelines API for [Elastic
net](http://en.wikipedia.org/wiki/Elastic_net_regularization), a hybrid
of $L_1$ and $L_2$ regularization proposed in [Zou et al, Regularization
and variable selection via the elastic
net](http://users.stat.umn.edu/~zouxx019/Papers/elasticnet.pdf).
Mathematically, it is defined as a convex combination of the $L_1$ and
the $L_2$ regularization terms:
`\[
\alpha~\lambda \|\wv\|_1 + (1-\alpha) \frac{\lambda}{2}\|\wv\|_2^2, \alpha \in [0, 1], \lambda \geq 0.
\]`
By setting $\alpha$ properly, elastic net contains both $L_1$ and $L_2$
regularization as special cases. For example, if a [linear
regression](https://en.wikipedia.org/wiki/Linear_regression) model is
trained with the elastic net parameter $\alpha$ set to $1$, it is
equivalent to a
[Lasso](http://en.wikipedia.org/wiki/Least_squares#Lasso_method) model.
On the other hand, if $\alpha$ is set to $0$, the trained model reduces
to a [ridge
regression](http://en.wikipedia.org/wiki/Tikhonov_regularization) model.
We implement Pipelines API for both linear regression and logistic
regression with elastic net regularization.

## Example: Logistic Regression

The following example shows how to train a logistic regression model
with elastic net regularization. `elasticNetParam` corresponds to
$\alpha$ and `regParam` corresponds to $\lambda$.

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

The `spark.ml` implementation of logistic regression also supports
extracting a summary of the model over the training set. Note that the
predictions and metrics which are stored as `Dataframe` in
`BinaryLogisticRegressionSummary` are annotated `@transient` and hence
only available on the driver.

<div class="codetabs">

<div data-lang="scala" markdown="1">

[`LogisticRegressionTrainingSummary`](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegressionTrainingSummary)
provides a summary for a
[`LogisticRegressionModel`](api/scala/index.html#org.apache.spark.ml.classification.LogisticRegressionModel).
Currently, only binary classification is supported and the
summary must be explicitly cast to
[`BinaryLogisticRegressionTrainingSummary`](api/scala/index.html#org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary).
This will likely change when multiclass classification is supported.

Continuing the earlier example:

{% highlight scala %}
// Extract the summary from the returned LogisticRegressionModel instance trained in the earlier example
val trainingSummary = lrModel.summary

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

<div data-lang="java" markdown="1">
[`LogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/LogisticRegressionTrainingSummary.html)
provides a summary for a
[`LogisticRegressionModel`](api/java/org/apache/spark/ml/classification/LogisticRegressionModel.html).
Currently, only binary classification is supported and the
summary must be explicitly cast to
[`BinaryLogisticRegressionTrainingSummary`](api/java/org/apache/spark/ml/classification/BinaryLogisticRegressionTrainingSummary.html).
This will likely change when multiclass classification is supported.

Continuing the earlier example:

{% highlight java %}
// Extract the summary from the returned LogisticRegressionModel instance trained in the earlier example
LogisticRegressionTrainingSummary trainingSummary = logRegModel.summary();

// Obtain the loss per iteration.
double[] objectiveHistory = trainingSummary.objectiveHistory();
for (double lossPerIteration : objectiveHistory) {
  System.out.println(lossPerIteration);
}

// Obtain the metrics useful to judge performance on test data.
// We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
// binary classification problem.
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

<div data-lang="python" markdown="1">
Logistic regression model summary is not yet supported in Python.
</div>

</div>

# Optimization

The optimization algorithm underlying the implementation is called
[Orthant-Wise Limited-memory
QuasiNewton](http://research-srv.microsoft.com/en-us/um/people/jfgao/paper/icml07scalable.pdf)
(OWL-QN). It is an extension of L-BFGS that can effectively handle L1
regularization and elastic net.

