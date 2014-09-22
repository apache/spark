---
layout: global
title: Artificial Neural Networks - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Artificial Neural Networks
---

# Introduction

This document describes the MLlib's Artificial Neural Network (ANN) implementation.

The implementation currently consist of the following files:

* 'ArtificialNeuralNetwork.scala': implements the ANN
* 'ANNSuite': implements automated tests for the ANN and its gradient
* 'ANNDemo': a demo that approximates three functions and shows a graphical representation of
the result

# Architecture and Notation

The file ArtificialNeuralNetwork.scala implements the ANN. The following picture shows the
architecture of a 3-layer ANN:

```
 +-------+
 |       |
 | N_0,0 |
 |       | 
 +-------+        +-------+
                  |       |
 +-------+        | N_0,1 |       +-------+
 |       |        |       |       |       |
 | N_1,0 |-       +-------+     ->| N_0,2 |
 |       | \ Wij1              /  |       |
 +-------+  --    +-------+  --   +-------+
               \  |       | / Wjk2
     :          ->| N_1,1 |-      +-------+
     :            |       |       |       |
     :            +-------+       | N_1,2 |
     :                            |       |
     :                :           +-------+
     :                :
     :                :                :
     :                : 
     :                :           +-------+
     :                :           |       |
     :                :           |N_K-1,2|
     :                            |       |
     :            +-------+       +-------+
     :            |       |
     :            |N_J-1,1|
                  |       |
 +-------+        +-------+
 |       | 
 |N_I-1,0|  
 |       |
 +-------+

 +-------+        +--------+
 |       |        |        |
 |   -1  |        |   -1   |
 |       |        |        |
 +-------+        +--------+

INPUT LAYER      HIDDEN LAYER    OUTPUT LAYER
```

The i-th node in layer l is denoted by N_{i,l}, both i and l starting with 0. The weight
between node i in layer l-1 and node j in layer l is denoted by Wijl. Layer 0 is the input
layer, whereas layer L is the output layer.

The ANN also implements bias units. These are nodes that always output the value -1. The bias
units are in all layers except the output layer. They act similar to other nodes, but do not
have input.

The value of node N_{j,l} is calculated  as follows:

`$N_{j,l} = g( \sum_{i=0}^{topology_l} W_{i,j,l)*N_{i,l-1} )$`

Where g is the sigmod function

`$g(t) = \frac{e^{\beta t} }{1+e^{\beta t}}$`

# LBFGS

MLlib uses the LBFGS algorithm for training. It minimises the following error function:

`$E = \sum_{k=0}^{K-1} (N_{k,L} - Y_k )^2$`

where Y_k is the target output given inputs N_{0,0} ... N_{I-1,0}.

# Implementation Details

## The `ArtificialNeuralNetwork` class

The `ArtificialNeuralNetwork` class has the following constructor:

`class ArtificialNeuralNetwork private(topology: Array[Int], maxNumIterations: Int,
convergenceTol: Double)`

* `topology` is an array of integers indicating then number of nodes per layer. For example, if
`topology` holds `(3, 5, 1)`, it means that there are three input nodes, five nodes in a single
hidden layer and 1 output node.
* `maxNumIterations` indicates the number of iterations after which the LBFGS algorithm must
have stopped.
* `convergenceTol` indicates the acceptable error, and if reached the LBFGS algorithm will
stop. A lower number of `convergenceTol` will give a higher precision.

There is also an object `ArtificialNeuralNetwork`. This object contains the training function.
There are six different instances of the training function, each for use with different
parameters. All take as the first parameter the RDD `input`, which contains pairs of input and
output vectors.

* `def train(input: RDD[(Vector, Vector)], topology: Array[Int], maxNumIterations: Int):
ArtificialNeuralNetworkModel`: starts training with random initial weights, and a default
`convergenceTol`=1e-5.
* `def train(input: RDD[(Vector, Vector)], topology: Array[Int], initialWeights: Vector,
maxNumIterations: Int): ArtificialNeuralNetworkModel`: starts training with given initial
weights, and a default `convergenceTol`=1e-5.
* `def train(input: RDD[(Vector, Vector)], model: ArtificialNeuralNetworkModel,
maxNumIterations: Int): ArtificialNeuralNetworkModel`: resumes training given an earlier
calculated model, and a default `convergenceTol`=1e-5.
* `def train(input: RDD[(Vector, Vector)], topology: Array[Int], maxNumIterations: Int,
convergenceTol: Double): ArtificialNeuralNetworkModel`: starts training with random initial
weights. Allows setting a customised `convergenceTol`.
* `def train(input: RDD[(Vector, Vector)], topology: Array[Int], initialWeights: Vector,
maxNumIterations: Int, convergenceTol: Double): ArtificialNeuralNetworkModel`: starts training
with given initial weights. Allows setting a customised `convergenceTol`.
* `def train(input: RDD[(Vector, Vector)], model: ArtificialNeuralNetworkModel,
maxNumIterations: Int, convergenceTol: Double): ArtificialNeuralNetworkModel`: resumes training
given an earlier calculated model. Allows setting a customised `convergenceTol`.

All training functions return the trained ANN using the class `ArtificialNeuralNetworkModel`.
This class has the following function:

* `predictV(testData: Vector): Vector` calculates the output vector given input vector
`testData`.

The weights use dby `predictV` come from the model.

## Training

We have chosen to implement the ANN with LBFGS as optimiser function. We compared it with
Statistical Gradient Descent. LBGFS was much faster, but in accordance is also earlier with
overfitting.

Science has provided many different strategies to train an ANN. Hence it is important that the
optimising functions in MLlib's ANN are interchangeable. A new optimisation strategy can be
implemented by creating a new class descending from ArtificialNeuralNetwork, and replacing the
optimiser, updater and possibly gradient as required.

# Demo and tests

Usage of MLlib's ANN is demonstrated through the 'ANNDemo' demo program. The program generates
three functions:

* f2d: x -> y
* f3d: (x,y) -> z
* f4d: t -> (x,y,z)

It will calculate an approximation of the target function, and show a graphical representation
of the training set and the results after applying the testing set.

In addition, there are the following tests:

* "ANN learns XOR function": tests that the ANN can properly approximate an XOR function.
* "Gradient of ANN": tests that the output of the ANN gradient is roughly equal to an
approximated gradient.

# Conclusion

The 'AritificalNeuralNetwork' class implements a Artificial Neural Network (ANN), using the
LBFGS algorithm. It takes as input an RDD of input/output values of type 'Vector', and returns
an object of type 'ArtificialNeuralNetworkModel' containing the parameters of the trained ANN.
The 'ArtificialNeuralNetworkModel' object can also be used to calculate results after training.

The training of an ANN can be interrupted and later continued, allowing intermediate inspection
of the results.

A demo program and tests for ANN are provided.
