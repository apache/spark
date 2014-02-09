---
layout: global
title: MLlib - Optimization
---

* Table of contents
{:toc}


# Gradient Descent Primitive

[Gradient descent](http://en.wikipedia.org/wiki/Gradient_descent) (along with
stochastic variants thereof) are first-order optimization methods that are
well-suited for large-scale and distributed computation. Gradient descent
methods aim to find a local minimum of a function by iteratively taking steps
in the direction of the negative gradient of the function at the current point,
i.e., the current parameter value. Gradient descent is included as a low-level
primitive in MLlib, upon which various ML algorithms are developed, and has the
following parameters:

* *gradient* is a class that computes the stochastic gradient of the function
being optimized, i.e., with respect to a single training example, at the
current parameter value. MLlib includes gradient classes for common loss
functions, e.g., hinge, logistic, least-squares.  The gradient class takes as
input a training example, its label, and the current parameter value. 
* *updater* is a class that updates weights in each iteration of gradient
descent. MLlib includes updaters for cases without regularization, as well as
L1 and L2 regularizers.
* *stepSize* is a scalar value denoting the initial step size for gradient
descent. All updaters in MLlib use a step size at the t-th step equal to
stepSize / sqrt(t). 
* *numIterations* is the number of iterations to run.
* *regParam* is the regularization parameter when using L1 or L2 regularization.
* *miniBatchFraction* is the fraction of the data used to compute the gradient
at each iteration.

Available algorithms for gradient descent:

* [GradientDescent](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent)

