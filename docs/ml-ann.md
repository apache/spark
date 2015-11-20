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
{% include_example scala/org/apache/spark/examples/ml/MultilayerPerceptronClassifierExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example java/org/apache/spark/examples/ml/JavaMultilayerPerceptronClassifierExample.java %}
</div>

<div data-lang="python" markdown="1">
{% include_example python/ml/multilayer_perceptron_classification.py %}
</div>

</div>
