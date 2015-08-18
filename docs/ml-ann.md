---
layout: global
title: Multilayer perceptron classifier - ML
displayTitle: <a href="ml-guide.html">ML</a> - Multilayer perceptron classifier
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


Multilayer perceptron classifier (MLPC) is a classifier based on the [feedforward artificial neural network](https://en.wikipedia.org/wiki/Feedforward_neural_network). 
MLPC consists of multiple layers of nodes. 
Each layer is fully connected to the next layer in the network. Nodes in the input layer represent the input data. All other nodes maps inputs to the outputs 
by performing linear combination of the inputs with the node's weights `$\wv$` and bias `$\bv$` and applying an activation function. 
It can be written in matrix form for MLPC with `$K+1$` layers as follows:
`\[
\mathrm{y}(\x) = \mathrm{f_K}(...\mathrm{f_2}(\wv_2^T\mathrm{f_1}(\wv_1^T \x+b_1)+b_2)...+b_K)
\]`
Nodes in intermediate layers use sigmoid (logistic) function:
`\[
\mathrm{f}(z_i) = \frac{1}{1 + e^{-z_i}}
\]`
Nodes in the output layer use softmax function:
`\[
\mathrm{f}(z_i) = \frac{e^{z_i}}{\sum_{k=1}^N e^{z_k}}
\]`
The number of nodes `$N$` in the output layer corresponds to the number of classes. 

MLPC employes backpropagation for learning the model. We use logistic loss function for optimization and L-BFGS as optimization routine.

**Examples**

<div class="codetabs">

<div data-lang="scala" markdown="1">

{% highlight scala %}

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row

// Load training data
val data = MLUtils.loadLibSVMFile(sc, "c:/ulanov/dev/spark/data/mllib/sample_multiclass_classification_data.txt").toDF()
// Split the data into train and test
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val train = splits(0)
val test = splits(1)
// specify layers for the neural network: 
// input layer of size 4 (features), two intermediate of size 5 and 4 and output of size 3 (classes)
val layers = Array[Int](4, 5, 4, 3)
// create the trainer and set its parameters
val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)
// fit the model to the data
val model = trainer.fit(train)
val result = model.transform(test)
val predictionAndLabels = result.select("prediction", "label")
// compute accuracy on the test set
val accuracy = predictionAndLabels.map{ case Row(p: Double, l: Double) => if (p == l) 1 else 0}.sum / predictionAndLabels.count
println("Accuracy:" + accuracy)
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.mllib.util.MLUtils;

public class Example {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
      .setAppName("Multilayer perceptron classifier");

    SparkContext sc = new SparkContext(conf);
    SQLContext sql = new SQLContext(sc);
    String path = "sample_libsvm_data.txt";

    // Load training data
    DataFrame training = sql.createDataFrame(MLUtils.loadLibSVMFile(sc, path).toJavaRDD(), LabeledPoint.class);

  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

{% highlight python %}
Sorry, Python example is not available yet

{% endhighlight %}

</div>

</div>
