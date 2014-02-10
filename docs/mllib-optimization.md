---
layout: global
title: MLlib - Optimization
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



# Mathematical Description

## (Sub)Gradient Descent
The simplest method to solve optimization problems of the form `$\min_{\wv \in\R^d} \; f(\wv)$`
is [gradient descent](http://en.wikipedia.org/wiki/Gradient_descent).
Such first-order optimization methods (including gradient descent and stochastic variants
thereof) are well-suited for large-scale and distributed computation.

Gradient descent methods aim to find a local minimum of a function by iteratively taking steps in
the direction of steepest descent, which is the negative of the derivative (called the
[gradient](http://en.wikipedia.org/wiki/Gradient)) of the function at the current point, i.e., at
the current parameter value.
If the objective function `$f$` is not differentiable at all arguments, but still convex, then a
*subgradient* 
is the natural generalization of the gradient, and assumes the role of the step direction.
In any case, computing a gradient or subgradient of `$f$` is expensive --- it requires a full
pass through the complete dataset, in order to compute the contributions from all loss terms.

## Stochastic (Sub)Gradient Descent (SGD)
Optimization problems whose objective function `$f$` is written as a sum are particularly
suitable to be solved using *stochastic subgradient descent (SGD)*. 
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
[L1Updater](api/mllib/index.html#org.apache.spark.mllib.optimization.L1Updater).


## Update Schemes for Distributed SGD
The SGD implementation in
[GradientDescent](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent) uses
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
([RDD](scala-programming-guide.html#resilient-distributed-datasets-rdds)), as well as the
computation of the sum of the partial results from each worker machine is performed by the
standard spark routines.

If the fraction of points `miniBatchFraction` is set to 1 (default), then the resulting step in
each iteration is exact (sub)gradient descent. In this case there is no randomness and no
variance in the used step directions.
On the other extreme, if `miniBatchFraction` is chosen very small, such that only a single point
is sampled, i.e. `$|S|=$ miniBatchFraction $\cdot n = 1$`, then the algorithm is equivalent to
standard SGD. In that case, the step direction depends from the uniformly random sampling of the
point.



# Implementation in MLlib

Gradient descent methods including stochastic subgradient descent (SGD) as
included as a low-level primitive in `MLlib`, upon which various ML algorithms 
are developed, see the 
<a href="mllib-classification-regression.html">classification and regression</a> 
section for example.

The SGD method
[GradientDescent.runMiniBatchSGD](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent)
has the following parameters:

* `gradient` is a class that computes the stochastic gradient of the function
being optimized, i.e., with respect to a single training example, at the
current parameter value. MLlib includes gradient classes for common loss
functions, e.g., hinge, logistic, least-squares.  The gradient class takes as
input a training example, its label, and the current parameter value. 
* `updater` is a class that performs the actual gradient descent step, i.e. 
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

* [GradientDescent.runMiniBatchSGD](api/mllib/index.html#org.apache.spark.mllib.optimization.GradientDescent)


