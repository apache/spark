---
layout: global
title: Machine Learning Library (MLlib)
---

MLlib is a Spark implementation of some common machine learning algorithms and utilities,
including classification, regression, clustering, collaborative
filtering, dimension reduction, as well as underlying optimization primitives:

* <a href="mllib-linear-algebra.html">Linear algebera</a>
  * vector and matrix
  * distributed matrix
* Classification and regression
  * <a href="mllib-generalized-linear-models.html">generalized linear models (GLMs)</a>
    * logistic regression
    * linear linear squares
  * <a href="mllib-linear-svm.html">linear support vector machine (SVM)</a>
  * <a href="mllib-decision-tree.html">decision tree</a>
  * <a href="mllib-naive-bayes.html">naive Bayes</a>
* <a href="mllib-collaborative-filtering.html">Collaborative Filtering</a>
  * alternating least squares (ALS)
* <a href="mllib-clustering.html">Clustering</a>
  * k-means
* <a href="mllib-dimentionality-reduction.html">Dimentionality reduction</a>
  * singular value decomposition (SVD)
  * principal component analysis (PCA)
* <a href="mllib-optimization.html">Optimization</a>
  * gradient descent
  * limited-memory BFGS (L-BFGS)

## Dependencies
MLlib uses the [jblas](https://github.com/mikiobraun/jblas) linear algebra library, which itself
depends on native Fortran routines. You may need to install the
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes. MLlib will throw a linking error if it cannot
detect these libraries automatically.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

## Migration guide
