---
layout: global
title: Naive Bayes - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Regression
---

## Regression
[Regression](http://en.wikipedia.org/wiki/Regression_analysis) is a statistical process
for estimating the relationships among variables. It includes many techniques for modeling
and analyzing several variables, when the focus is on the relationship between
a dependent variable and one or more independent variables.

## Isotonic regression
[Isotonic regression](http://en.wikipedia.org/wiki/Isotonic_regression)
belongs to the family of regression algorithms. Formally isotonic regression is a problem where
given a finite set of real numbers `$Y = {y_1, y_2, ..., y_n}$` representing observed responses
and `$X = {x_1, x_2, ..., x_n}$` the unknown response values to be fitted
finding a function that minimises

`\begin{equation}
  f(x) = \sum_{i=1}^n w_i (y_i - x_i)^2
\end{equation}`

with respect to complete order subject to
`$x_1\le x_2\le ...\le x_n$` where `$w_i$` are positive weights.
The resulting function is called isotonic regression and it is unique.
It can be viewed as least squares problem under order restriction.
Essentially isotonic regression is a
[monotonic function](http://en.wikipedia.org/wiki/Monotonic_function)
best fitting the original data points.

MLlib supports a
[pool adjacent violators algorithm](http://www.stat.cmu.edu/~ryantibs/papers/neariso.pdf)
which uses an approach to
[parallelizing isotonic regression](http://softlib.rice.edu/pub/CRPC-TRs/reports/CRPC-TR96640.pdf).
The training input is a RDD of
[tuples](http://www.scala-lang.org/api/2.10.3/index.html#scala.Tuple3)
of three double values that represent label, feature and weight in this order.
Additionally IsotonicRegression algorithm has one optional parameter
called $isotonic$ defaulting to true.
This argument specifies if the isotonic regression is
isotonic (monotonically increasing) or antitonic (monotonically decreasing).

Training returns an IsotonicRegressionModel that can be used to predict
labels for both known and unknown features. The result of isotonic regression
is treated as piecewise linear function. The rules the prediction uses therefore are:

* If testData exactly matches a boundary then associated prediction is returned.
  In case there are multiple predictions with the same boundary then one of them
  is returned. Which one is undefined (same as java.util.Arrays.binarySearch).
* If testData is lower or higher than all boundaries then first or last prediction
  is returned respectively. In case there are multiple predictions with the same
  boundary then the lowest or highest is returned respectively.
* If testData falls between two values in boundary array then prediction is treated
  as piecewise linear function and interpolated value is returned. In case there are
  multiple values with the same boundary then the same rules as in previous point are used.

### Examples

<div class="codetabs">
<div data-lang="scala" markdown="1">
Data are read from a csv file where each line has a format label,feature,weight
i.e. 4710.28,500.00,1.00. The data are split to training and testing set.
Model is created using the training set and a mean squared error is calculated from the predicted
labels and real labels in the test set.

{% highlight scala %}
import org.apache.spark.mllib.regression.IsotonicRegression

val data = sc.textFile("data/mllib/sample_isotonic_regression_data.csv")

// Create label, feature, weight tuples from input data.
val parsedData = data.map { line =>
  val parts = line.split(',').map(_.toDouble)
  (parts(0), parts(1), parts(2))
}

// Split data into training (60%) and test (40%) sets.
val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0)
val test = splits(1)

// Create isotonic regression model from training data.
// Isotonic parameter defaults to true so it is only shown for demonstration
val model = new IsotonicRegression().setIsotonic(true).run(training)

// Create tuples of predicted and real labels.
val predictionAndLabel = test.map { point =>
  val predictedLabel = model.predict(point._2)
  (predictedLabel, point._1)
}

// Calculate mean squared error between predicted and real labels.
val meanSquaredError = predictionAndLabel.map{case(p, l) => math.pow((p - l), 2)}.mean()
println("Mean Squared Error = " + meanSquaredError)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
Data are read from a csv file where each line has a format label,feature,weight
i.e. 4710.28,500.00,1.00. The data are split to training and testing set.
Model is created using the training set and a mean squared error is calculated from the predicted
labels and real labels in the test set.

{% highlight java %}
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.IsotonicRegressionModel;
import scala.Tuple2;
import scala.Tuple3;

JavaRDD<String> data = sc.textFile("data/mllib/sample_isotonic_regression_data.csv");

// Create label, feature, weight tuples from input data.
JavaRDD<Tuple3<Double, Double, Double>> parsedData = data.map(
  new Function<String, Tuple3<Double, Double, Double>>() {
    public Tuple3<Double, Double, Double> call(String line) {
      String[] parts = line.split(",");

      return new Tuple3<>(new Double(parts[0]), new Double(parts[1]), new Double(parts[2]));
    }
  }
);

// Split data into training (60%) and test (40%) sets.
JavaRDD<Tuple3<Double, Double, Double>>[] splits = parsedData.randomSplit(new double[] {0.6, 0.4}, 11L);
JavaRDD<Tuple3<Double, Double, Double>> training = splits[0];
JavaRDD<Tuple3<Double, Double, Double>> test = splits[1];

// Create isotonic regression model from training data.
// Isotonic parameter defaults to true so it is only shown for demonstration
IsotonicRegressionModel model = new IsotonicRegression().setIsotonic(true).run(training);

// Create tuples of predicted and real labels.
JavaPairRDD<Double, Double> predictionAndLabel = test.mapToPair(
  new PairFunction<Tuple3<Double, Double, Double>, Double, Double>() {
    @Override public Tuple2<Double, Double> call(Tuple3<Double, Double, Double> point) {
      Double predictedLabel = model.predict(point._2());
      return new Tuple2<Double, Double>(predictedLabel, point._1());
    }
  }
);

// Calculate mean squared error between predicted and real labels.
Double meanSquaredError = new JavaDoubleRDD(predictionAndLabel.map(
  new Function<Tuple2<Double, Double>, Object>() {
    @Override public Object call(Tuple2<Double, Double> pl) {
      return Math.pow(pl._1() - pl._2(), 2);
    }
  }
).rdd()).mean();

System.out.println("Mean Squared Error = " + meanSquaredError);
{% endhighlight %}
</div>
</div>