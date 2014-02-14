---
layout: global
title: Machine Learning Library (MLlib)
---


MLlib is a Spark implementation of some common machine learning (ML)
functionality, as well associated tests and data generators.  MLlib
currently supports four common types of machine learning problem settings,
namely, binary classification, regression, clustering and collaborative
filtering, as well as an underlying gradient descent optimization primitive.

# Available Methods
The following links provide a detailed explanation of the methods and usage examples for each of them: 

* <a href="mllib-classification-regression.html">Classification and Regression</a>
  * Binary Classification
    * SVM (L1 and L2 regularized)
    * Logistic Regression (L1 and L2 regularized)
  * Linear Regression
    * Least Squares
    * Lasso
    * Ridge Regression
* <a href="mllib-clustering.html">Clustering</a>
  * k-Means
* <a href="mllib-collaborative-filtering.html">Collaborative Filtering</a>
  * Matrix Factorization using Alternating Least Squares
* <a href="mllib-optimization.html">Optimization</a>
  * Gradient Descent and Stochastic Gradient Descent
* <a href="mllib-linear-algebra.html">Linear Algebra</a>
  * Singular Value Decomposition

# Dependencies
MLlib uses the [jblas](https://github.com/mikiobraun/jblas) linear algebra library, which itself
depends on native Fortran routines. You may need to install the 
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes. MLlib will throw a linking error if it cannot 
detect these libraries automatically.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.7 or newer
and Python 2.7.

