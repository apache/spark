---
layout: global
title: Machine Learning Library (MLlib)
---

MLlib is a Spark implementation of some common machine learning algorithms and utilities,
including classification, regression, clustering, collaborative
filtering, dimension reduction, as well as underlying optimization primitives.

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
  * Decision Tree (for classification and regression)
* <a href="mllib-clustering.html">Clustering</a>
  * k-Means
* <a href="mllib-collaborative-filtering.html">Collaborative Filtering</a>
  * matrix factorization using alternating least squares
* <a href="mllib-clustering.html">Clustering</a>
  * k-means
* <a href="mllib-linear-algebra.html">Distributed Linear Algebra</a>
  * tall-and-skinny matrices
    * column summary statistics
    * singular value decomposition (SVD)
    * principal component analysis (PCA)
* <a href="mllib-optimization.html">Optimization</a>
  * gradient descent and stochastic gradient descent
  * L-BFGS

# Dependencies
MLlib uses the [jblas](https://github.com/mikiobraun/jblas) linear algebra library, which itself
depends on native Fortran routines. You may need to install the
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes. MLlib will throw a linking error if it cannot
detect these libraries automatically.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.
