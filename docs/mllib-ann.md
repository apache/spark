---
layout: global
title: Artificial Neural Networks - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Artificial Neural Networks
---

# Introduction

This document describes the MLlib's Artificial Neural Network (ANN) implementation.

The implementation currently consist of the following files:

* 'ParallelANN.scala': implements the ANN
* 'GeneralizedSteepestDescentAlgorithm.scala': provides an abstract class and model as basis for 'ParallelANN'.

In addition, there is a demo/test available:

* 'TestParallelANN.scala': tests parallel ANNs for various functions
* 'TestParallelANNgraphics.scala': graphical output for 'TestParallelANN.scala'

# Architecture and Notation

The file ParallelANN.scala implements a three-layer ANN with the following architecture:

```
 +-------+
 |       |
 |  X_0  |
 |       | 
 +-------+       +-------+
                 |       |
 +-------+       |  H_0  |      +-------+
 |       |       |       |      |       |
 |  X_1  |-      +-------+    ->|  O_0  |
 |       | \ Vij             /  |       |
 +-------+  -    +-------+  -   +-------+
              \  |       | / Wjk
     :         ->|  H_1  |-     +-------+
     :           |       |      |       |
     :           +-------+      |  O_1  |
     :                          |       |
     :               :          +-------+
     :               :
     :               :              :
     :               : 
     :               :          +-------+
     :               :          |       |
     :               :          | O_K-1 |
     :                          |       |
     :           +-------+      +-------+
     :           |       |
     :           | H_J-1 |
                 |       |
 +-------+       +-------+
 |       | 
 | X_I-1 |  
 |       |
 +-------+

 +-------+      +--------+
 |       |      |        |
 |   -1  |      |   -1   |
 |       |      |        |
 +-------+      +--------+

INPUT LAYER     HIDDEN LAYER    OUTPUT LAYER
```

The nodes X_0 to X_{I-1} are the I input nodes. The nodes H_0 to H_{J-1} are the J hidden nodes and the nodes O_0 to O_{K-1} are the K output nodes. Between each input node X_i and hidden node H_j there is a weight V_{ij}. Likewise, between each hidden node H_j and each output node O_k is a weight W_{jk}. 

The ANN also implements two bias units. These are nodes that always output the value -1. The bias units are in the input and in the hidden layer. They act as normal nodes, except that the bias unit in the hidden layer has no input. The bias units can also be denoted by X_I and H_J.

The value of a hidden node H_j is calculated as follows:

`$H_j = g ( \sum_{i=0}^{I} X_i*V_{i,j} )$`

Likewise, the value of the output node O_k is calculated as follows:

`$O_k = g( \sum_{j=0}^{J} H_j*W_{j,k} )$`

Where g is the sigmod function

`$g(t) = \frac{e^{\beta t} }{1+e^{\beta t}}$`

and `$\beta` the learning rate.

# Gradient descent

Currently, the MLlib uses gradent descent for training. This means that the weights V_{ij} and W_{jk} are updated by adding a fraction of the gradient to V_{ij} and W_{jk} of the following function:

`$E = \sum_{k=0}^{K-1} (O_k - Y_k )^2$`

where Y_k is the target output given inputs X_0 ... X_{I-1}

Calculations provide that:

`$\frac{\partial E}{\partial W_{jk}} = 2 (O_k-Y_k) \cdot H_j \cdot g' \left( \sum_{m=0}^{J} W_{mk} H_m \right)$`

and

`$\frac{\partial E}{\partial V_{ij}} = 2 \sum_{k=0}^{K-1} \left( (O_k - Y_k)  \cdot X_i \cdot W_{jk} \cdot g'\left( \sum_{n=0}^{J} W_{nk} H_n \right) g'\left( \sum_{m=0}^{I} V_{mj} X_i \right) \right)$`

The training step consists of the two operations

`$V_{ij} = V_{ij} - \epsilon \frac{\partial E}{\partial V_{ij}}$`

and

`$W_{jk} = W_{jk} - \epsilon \frac{\partial E}{\partial W_{jk}}$`

where `$\epsilon$` is the step size.

# Implementation Details

## The 'ParallelANN' class

The 'ParallelANN' class is the main class of the ANN. This class uses a trait 'ANN', which includes functions for calculating the hidden layer ('computeHidden') and calculation of the output ('computeValues'). The output of 'computeHidden' includes the bias node in the hidden layer, such that it does not need to handle the hidden bias node differently.

The 'ParallelANN' class has the following constructors:

* `ParallelANN( stepSize, numIterations, miniBatchFraction, noInput, noHidden, noOutput, beta )`
* `ParallelANN()`: assumes 'stepSize'=1.0, 'numIterations'=100, 'miniBatchFraction'=1.0, 'noInput'=1, 'noHidden'=5, noOutput'=1, 'beta'=1.0.
* `ParallelANN( noHidden )`: as 'ParallelANN()', but allows specification of 'noHidden'
* `ParallelANN( noInput, noHidden )`: as 'ParallelANN()', but allows specification of number of 'noInput' and 'noHidden'
* `ParallelANN( noInput, noHidden, noOutput )`: as 'ParallelANN()', but allows specification of 'noInput', 'noHidden' and 'noOutput'

The number of input nodes I is stored in the variable 'noInput', the number of hidden nodes J is stored in 'noHidden' and the number of output nodes K is stored in 'noOutput'. 'beta' contains the value of '$\beta$' for the sigmoid function.

The parameters 'stepSize', 'numIterations' and 'miniBatchFraction' are of use for the Statistical Gradient Descent function.

In addition, it has a single vector 'weights' corresponding to V_{ij} and W_{jk}. The mapping of V_{ij} and W_{jk} into 'weights' is as follows:

* V_{ij} -> `weights[  i + j*(noInput+1) ]$`
* W_{jk} -> `weights[ (noInput+1)*noHidden + j + k*(noHidden+1) ]$`

The training function carries the name 'train'. It can take various inputs:

* `def train( rdd: RDD[(Vector,Vector)] )`: starts a complete new training session and generates a new ANN.
* `def train( rdd: RDD[(Vector,Vector)], model: ParallelANNModel )`: continues a training session with an existing ANN.
* `def train( rdd: RDD[(Vector,Vector)], weights: Vector )`: starts a training session using initial weights as indicated by 'weights'.

The input of the training function is an RDD with (input/output) training pairs, each input and output being stored as a 'Vector'. The training function returns a variable of from class 'ParallelANNModel', as described below.

## The 'ParallelANNModel' class

All information needed for the ANN is stored in the 'ParallelANNModel' class. The training function 'train' from 'ParallelANN' returns an object from the 'ParallelANNModel' class.

The information in 'parallelANNModel' consist of the weights, the number of input, hidden and output nodes, as well as two functions 'predictPoint' and 'predictPointV'.

The 'predictPoint' function is used to calculate a single output value as a 'Double'. If the output of the ANN actually is a vector, it returns just the first element of the vector, that is O_0. The output of the 'predictPointV' is of type 'Vector', and returns all K output values.

## The 'GeneralizedSteepestDescentAlgorithm' class

The 'GeneralizedSteepestDescendAlgorithm' class is based on the 'GeneralizedLinearAlgorithm' class. The main difference is that the 'GeneralizedSteepestDescentAlgorithm' is based on output values of type 'Vector', whereas 'GeneralizedLinearAlgorithm' is based of output values of type 'Double'. The new class was needed, because an ANN ideally outputs multiple values, hence a 'Vector'.

## Training

Science has provided many different strategies to train an ANN. Hence it is important that the optimising functions in MLlib's ANN are interchangeable. The ParallelANN class has a variable 'optimizer', which is currently set to a 'GradientDescent' optimising class. The 'GradientDescent' optimising class implements a stochastic gradient descent method, and is also used for other optimisation technologies in Spark. It is expected that other optimising functions will be defined for Spark, and these can be stored in the 'optimizer' variable.

# Demo/test

Usage of MLlib's ANN is demonstrated through the 'TestParallelANN' demo program. The program generates three functions:

* f2d: x -> y
* f3d: (x,y) -> z
* f4d: t -> (x,y,z)

When the program is given the Java argument 'graph', it will show a graphical representation of the target function and the latest values.

# Conclusion

The 'ParallelANN' class implements a Artificial Neural Network (ANN), using the stochastic gradient descent method. It takes as input an RDD of input/output values of type 'Vector', and returns an object of type 'ParallelANNModel' containing the parameters of the trained ANN. The 'ParallelANNModel' object can also be used to calculate results after training.

The training of an ANN can be interrupted and later continued, allowing intermediate inspection of the results.

A demo program for ANN is provided.
