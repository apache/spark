---
layout: global
title: Optimization - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Optimization
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



## Mathematical description

### Gradient descent
The simplest method to solve optimization problems of the form `$\min_{\wv \in\R^d} \; f(\wv)$`
is [gradient descent](http://en.wikipedia.org/wiki/Gradient_descent).
Such first-order optimization methods (including gradient descent and stochastic variants
thereof) are well-suited for large-scale and distributed computation.

Gradient descent methods aim to find a local minimum of a function by iteratively taking steps in
the direction of steepest descent, which is the negative of the derivative (called the
[gradient](http://en.wikipedia.org/wiki/Gradient)) of the function at the current point, i.e., at
the current parameter value.
If the objective function `$f$` is not differentiable at all arguments, but still convex, then a
*sub-gradient* 
is the natural generalization of the gradient, and assumes the role of the step direction.
In any case, computing a gradient or sub-gradient of `$f$` is expensive --- it requires a full
pass through the complete dataset, in order to compute the contributions from all loss terms.

### Stochastic gradient descent (SGD)
Optimization problems whose objective function `$f$` is written as a sum are particularly
suitable to be solved using *stochastic gradient descent (SGD)*. 
In our case, for the optimization formulations commonly used in <a
href="mllib-classification-regression.html">supervised machine learning</a>,
`\begin{equation}
    f(\wv) := 
    \lambda\, R(\wv) +
    \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i) 
    \label{eq:regPrimal}
    \ .
\end{equation}`
this is especially natural, because the loss is written as an average of the individual losses
coming from each datapoint.

A stochastic subgradient is a randomized choice of a vector, such that in expectation, we obtain
a true subgradient of the original objective function.
Picking one datapoint `$i\in[1..n]$` uniformly at random, we obtain a stochastic subgradient of
`$\eqref{eq:regPrimal}$`, with respect to `$\wv$` as follows:
`\[
f'_{\wv,i} := L'_{\wv,i} + \lambda\, R'_\wv \ ,
\]`
where `$L'_{\wv,i} \in \R^d$` is a subgradient of the part of the loss function determined by the
`$i$`-th datapoint, that is `$L'_{\wv,i} \in \frac{\partial}{\partial \wv}  L(\wv;\x_i,y_i)$`.
Furthermore, `$R'_\wv$` is a subgradient of the regularizer `$R(\wv)$`, i.e. `$R'_\wv \in
\frac{\partial}{\partial \wv} R(\wv)$`. The term `$R'_\wv$` does not depend on which random
datapoint is picked.
Clearly, in expectation over the random choice of `$i\in[1..n]$`, we have that `$f'_{\wv,i}$` is
a subgradient of the original objective `$f$`, meaning that `$\E\left[f'_{\wv,i}\right] \in
\frac{\partial}{\partial \wv} f(\wv)$`.

Running SGD now simply becomes walking in the direction of the negative stochastic subgradient
`$f'_{\wv,i}$`, that is
`\begin{equation}\label{eq:SGDupdate}
\wv^{(t+1)} := \wv^{(t)}  - \gamma \; f'_{\wv,i} \ .
\end{equation}`
**Step-size.**
The parameter `$\gamma$` is the step-size, which in the default implementation is chosen
decreasing with the square root of the iteration counter, i.e. `$\gamma := \frac{s}{\sqrt{t}}$`
in the `$t$`-th iteration, with the input parameter `$s=$ stepSize`. Note that selecting the best
step-size for SGD methods can often be delicate in practice and is a topic of active research.

**Gradients.**
A table of (sub)gradients of the machine learning methods implemented in MLlib, is available in
the <a href="mllib-classification-regression.html">classification and regression</a> section.


**Proximal Updates.**
As an alternative to just use the subgradient `$R'(\wv)$` of the regularizer in the step
direction, an improved update for some cases can be obtained by using the proximal operator
instead.
For the L1-regularizer, the proximal operator is given by soft thresholding, as implemented in
[L1Updater](api/scala/index.html#org.apache.spark.mllib.optimization.L1Updater).


### Update schemes for distributed SGD
The SGD implementation in
[GradientDescent](api/scala/index.html#org.apache.spark.mllib.optimization.GradientDescent) uses
a simple (distributed) sampling of the data examples.
We recall that the loss part of the optimization problem `$\eqref{eq:regPrimal}$` is
`$\frac1n \sum_{i=1}^n L(\wv;\x_i,y_i)$`, and therefore `$\frac1n \sum_{i=1}^n L'_{\wv,i}$` would
be the true (sub)gradient.
Since this would require access to the full data set, the parameter `miniBatchFraction` specifies
which fraction of the full data to use instead.
The average of the gradients over this subset, i.e.
`\[
\frac1{|S|} \sum_{i\in S} L'_{\wv,i} \ ,
\]`
is a stochastic gradient. Here `$S$` is the sampled subset of size `$|S|=$ miniBatchFraction
$\cdot n$`.

In each iteration, the sampling over the distributed dataset
([RDD](programming-guide.html#resilient-distributed-datasets-rdds)), as well as the
computation of the sum of the partial results from each worker machine is performed by the
standard spark routines.

If the fraction of points `miniBatchFraction` is set to 1 (default), then the resulting step in
each iteration is exact (sub)gradient descent. In this case there is no randomness and no
variance in the used step directions.
On the other extreme, if `miniBatchFraction` is chosen very small, such that only a single point
is sampled, i.e. `$|S|=$ miniBatchFraction $\cdot n = 1$`, then the algorithm is equivalent to
standard SGD. In that case, the step direction depends from the uniformly random sampling of the
point.

### Limited-memory BFGS (L-BFGS)
[L-BFGS](http://en.wikipedia.org/wiki/Limited-memory_BFGS) is an optimization 
algorithm in the family of quasi-Newton methods to solve the optimization problems of the form 
`$\min_{\wv \in\R^d} \; f(\wv)$`. The L-BFGS method approximates the objective function locally as a 
quadratic without evaluating the second partial derivatives of the objective function to construct the 
Hessian matrix. The Hessian matrix is approximated by previous gradient evaluations, so there is no 
vertical scalability issue (the number of training features) when computing the Hessian matrix 
explicitly in Newton's method. As a result, L-BFGS often achieves rapider convergence compared with 
other first-order optimization. 

## Implementation in MLlib

### Gradient descent and stochastic gradient descent
Gradient descent methods including stochastic subgradient descent (SGD) as
included as a low-level primitive in `MLlib`, upon which various ML algorithms 
are developed, see the 
<a href="mllib-linear-methods.html">linear methods</a> 
section for example.

The SGD class
[GradientDescent](api/scala/index.html#org.apache.spark.mllib.optimization.GradientDescent)
sets the following parameters:

* `Gradient` is a class that computes the stochastic gradient of the function
being optimized, i.e., with respect to a single training example, at the
current parameter value. MLlib includes gradient classes for common loss
functions, e.g., hinge, logistic, least-squares.  The gradient class takes as
input a training example, its label, and the current parameter value. 
* `Updater` is a class that performs the actual gradient descent step, i.e. 
updating the weights in each iteration, for a given gradient of the loss part.
The updater is also responsible to perform the update from the regularization 
part. MLlib includes updaters for cases without regularization, as well as
L1 and L2 regularizers.
* `stepSize` is a scalar value denoting the initial step size for gradient
descent. All updaters in MLlib use a step size at the t-th step equal to
`stepSize $/ \sqrt{t}$`. 
* `numIterations` is the number of iterations to run.
* `regParam` is the regularization parameter when using L1 or L2 regularization.
* `miniBatchFraction` is the fraction of the total data that is sampled in 
each iteration, to compute the gradient direction.

Available algorithms for gradient descent:

* [GradientDescent](api/scala/index.html#org.apache.spark.mllib.optimization.GradientDescent)

### L-BFGS
L-BFGS is currently only a low-level optimization primitive in `MLlib`. If you want to use L-BFGS in various 
ML algorithms such as Linear Regression, and Logistic Regression, you have to pass the gradient of objective
function, and updater into optimizer yourself instead of using the training APIs like 
[LogisticRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD).
See the example below. It will be addressed in the next release. 

The L1 regularization by using 
[L1Updater](api/scala/index.html#org.apache.spark.mllib.optimization.L1Updater) will not work since the 
soft-thresholding logic in L1Updater is designed for gradient descent. See the developer's note.

The L-BFGS method
[LBFGS.runLBFGS](api/scala/index.html#org.apache.spark.mllib.optimization.LBFGS)
has the following parameters:

* `Gradient` is a class that computes the gradient of the objective function
being optimized, i.e., with respect to a single training example, at the
current parameter value. MLlib includes gradient classes for common loss
functions, e.g., hinge, logistic, least-squares.  The gradient class takes as
input a training example, its label, and the current parameter value. 
* `Updater` is a class that computes the gradient and loss of objective function 
of the regularization part for L-BFGS. MLlib includes updaters for cases without 
regularization, as well as L2 regularizer. 
* `numCorrections` is the number of corrections used in the L-BFGS update. 10 is 
recommended.
* `maxNumIterations` is the maximal number of iterations that L-BFGS can be run.
* `regParam` is the regularization parameter when using regularization.

The `return` is a tuple containing two elements. The first element is a column matrix
containing weights for every feature, and the second element is an array containing 
the loss computed for every iteration.

Here is an example to train binary logistic regression with L2 regularization using
L-BFGS optimizer. 

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionModel

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
val numFeatures = data.take(1)(0).features.size

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)

// Append 1 into the training data as intercept.
val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()

val test = splits(1)

// Run training algorithm to build the model
val numCorrections = 10
val convergenceTol = 1e-4
val maxNumIterations = 20
val regParam = 0.1
val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))

val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
  training,
  new LogisticGradient(),
  new SquaredL2Updater(),
  numCorrections,
  convergenceTol,
  maxNumIterations,
  regParam,
  initialWeightsWithIntercept)

val model = new LogisticRegressionModel(
  Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
  weightsWithIntercept(weightsWithIntercept.size - 1))

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set.
val scoreAndLabels = test.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Loss of each step in training process")
loss.foreach(println)
println("Area under ROC = " + auROC)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;
import java.util.Random;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.optimization.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class LBFGSExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("L-BFGS Example");
    SparkContext sc = new SparkContext(conf);
    String path = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();
    int numFeatures = data.take(1).get(0).features().size();
    
    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint> trainingInit = data.sample(false, 0.6, 11L);
    JavaRDD<LabeledPoint> test = data.subtract(trainingInit);
    
    // Append 1 into the training data as intercept.
    JavaRDD<Tuple2<Object, Vector>> training = data.map(
      new Function<LabeledPoint, Tuple2<Object, Vector>>() {
        public Tuple2<Object, Vector> call(LabeledPoint p) {
          return new Tuple2<Object, Vector>(p.label(), MLUtils.appendBias(p.features()));
        }
      });
    training.cache();

    // Run training algorithm to build the model.
    int numCorrections = 10;
    double convergenceTol = 1e-4;
    int maxNumIterations = 20;
    double regParam = 0.1;
    Vector initialWeightsWithIntercept = Vectors.dense(new double[numFeatures + 1]);

    Tuple2<Vector, double[]> result = LBFGS.runLBFGS(
      training.rdd(),
      new LogisticGradient(),
      new SquaredL2Updater(),
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept);
    Vector weightsWithIntercept = result._1();
    double[] loss = result._2();

    final LogisticRegressionModel model = new LogisticRegressionModel(
      Vectors.dense(Arrays.copyOf(weightsWithIntercept.toArray(), weightsWithIntercept.size() - 1)),
      (weightsWithIntercept.toArray())[weightsWithIntercept.size() - 1]);

    // Clear the default threshold.
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
      public Tuple2<Object, Object> call(LabeledPoint p) {
        Double score = model.predict(p.features());
        return new Tuple2<Object, Object>(score, p.label());
      }
    });

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics = 
      new BinaryClassificationMetrics(scoreAndLabels.rdd());
    double auROC = metrics.areaUnderROC();
     
    System.out.println("Loss of each step in training process");
    for (double l : loss)
      System.out.println(l);
    System.out.println("Area under ROC = " + auROC);
  }
}
{% endhighlight %}
</div>
</div>
#### Developer's note
Since the Hessian is constructed approximately from previous gradient evaluations, 
the objective function can not be changed during the optimization process. 
As a result, Stochastic L-BFGS will not work naively by just using miniBatch; 
therefore, we don't provide this until we have better understanding.

* `Updater` is a class originally designed for gradient decent which computes 
the actual gradient descent step. However, we're able to take the gradient and 
loss of objective function of regularization for L-BFGS by ignoring the part of logic
only for gradient decent such as adaptive step size stuff. We will refactorize
this into regularizer to replace updater to separate the logic between 
regularization and step update later. 
