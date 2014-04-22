---
layout: global
title: Machine Learning Library (MLlib)
---


MLlib is a Spark implementation of some common machine learning (ML)
functionality, as well associated tests and data generators.  MLlib
currently supports four common types of machine learning problem settings,
namely classification, regression, clustering and collaborative filtering,
as well as an underlying gradient descent optimization primitive and several
linear algebra methods.

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
  * Matrix Factorization using Alternating Least Squares
* <a href="mllib-optimization.html">Optimization</a>
  * Gradient Descent and Stochastic Gradient Descent
* <a href="mllib-linear-algebra.html">Linear Algebra</a>
  * Singular Value Decomposition
  * Principal Component Analysis

# Data Types

Most MLlib algorithms operate on RDDs containing vectors. In Java and Scala, the
[Vector](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector) class is used to
represent vectors. You can create either dense or sparse vectors using the
[Vectors](api/mllib/index.html#org.apache.spark.mllib.linalg.Vectors$) factory.

In Python, MLlib can take the following vector types:

* [NumPy](http://www.numpy.org) arrays
* Standard Python lists (e.g. `[1, 2, 3]`)
* The MLlib [SparseVector](api/pyspark/pyspark.mllib.linalg.SparseVector-class.html) class
* [SciPy sparse matrices](http://docs.scipy.org/doc/scipy/reference/sparse.html)

For efficiency, we recommend using NumPy arrays over lists, and using the
[CSC format](http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csc_matrix.html#scipy.sparse.csc_matrix)
for SciPy matrices, or MLlib's own SparseVector class.

Several other simple data types are used throughout the library, e.g. the LabeledPoint
class ([Java/Scala](api/mllib/index.html#org.apache.spark.mllib.regression.LabeledPoint),
[Python](api/pyspark/pyspark.mllib.regression.LabeledPoint-class.html)) for labeled data.

# Dependencies
MLlib uses the [Breeze](https://github.com/scalanlp/breeze) linear algebra library, which depends on
[netlib-java](https://github.com/fommil/netlib-java) as a wrapper for low-level BLAS, LAPACK and ARPACK
that performs as fast as the C / Fortran interfaces. Although netlib-java provides JVM implementation of
linear algebra, [F2J](http://icl.cs.utk.edu/f2j/) to ensure full portability, the performance is not optimized.
Users can install [commercial or open source BLAS/LAPACK implementation](http://en.wikipedia.org/wiki/Basic_Linear_Algebra_Subprograms#Implementations)
or enable the netlib-java self-contained native builds using the reference Fortran implementation from
[netlib.org](http://www.netlib.org/) by installing libgfortran3 in Linux. See the netlib-java documentation for detail.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.
