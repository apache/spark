---
layout: global
title: MLlib - Linear Algebra
---

* Table of contents
{:toc}


# Singular Value Decomposition
Singular Value `Decomposition` for Tall and Skinny matrices.
Given an `$m \times n$` matrix `$A$`, we can compute matrices `$U,S,V$` such that

`\[
 A = U \cdot S \cdot V^T
 \]`

There is no restriction on m, but we require n^2 doubles to
fit in memory locally on one machine.
Further, n should be less than m.

The decomposition is computed by first computing `$A^TA = V S^2 V^T$`,
computing SVD locally on that (since `$n \times n$` is small),
from which we recover `$S$` and `$V$`.
Then we compute U via easy matrix multiplication
as `$U =  A \cdot V \cdot S^{-1}$`.

Only singular vectors associated with largest k singular values
are recovered. If there are k
such values, then the dimensions of the return will be:

* `$S$` is `$k \times k$` and diagonal, holding the singular values on diagonal.
* `$U$` is `$m \times k$` and satisfies `$U^T U = \mathop{eye}(k)$`.
* `$V$` is `$n \times k$` and satisfies `$V^T V = \mathop{eye}(k)$`.

All input and output is expected in sparse matrix format, 0-indexed
as tuples of the form ((i,j),value) all in
SparseMatrix RDDs. Below is example usage.

{% highlight scala %}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SVD
import org.apache.spark.mllib.linalg.SparseMatrix
import org.apache.spark.mllib.linalg.MatrixEntry

// Load and parse the data file
val data = sc.textFile("mllib/data/als/test.data").map { line =>
  val parts = line.split(',')
  MatrixEntry(parts(0).toInt, parts(1).toInt, parts(2).toDouble)
}
val m = 4
val n = 4
val k = 1

// recover largest singular vector
val decomposed = SVD.sparseSVD(SparseMatrix(data, m, n), k)
val = decomposed.S.data

println("singular values = " + s.toArray.mkString)
{% endhighlight %}
