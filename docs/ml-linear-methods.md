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
