---
layout: global
title: Dimensionality Reduction - RDD-based API
displayTitle: Dimensionality Reduction - RDD-based API
---

* Table of contents
{:toc}

[Dimensionality reduction](http://en.wikipedia.org/wiki/Dimensionality_reduction) is the process 
of reducing the number of variables under consideration.
It can be used to extract latent features from raw and noisy features
or compress data while maintaining the structure.
`spark.mllib` provides support for dimensionality reduction on the <a href="mllib-data-types.html#rowmatrix">RowMatrix</a> class.

## Singular value decomposition (SVD)

[Singular value decomposition (SVD)](http://en.wikipedia.org/wiki/Singular_value_decomposition)
factorizes a matrix into three matrices: $U$, $\Sigma$, and $V$ such that

`\[
A = U \Sigma V^T,
\]`

where 

* $U$ is an orthonormal matrix, whose columns are called left singular vectors,
* $\Sigma$ is a diagonal matrix with non-negative diagonals in descending order, 
  whose diagonals are called singular values,
* $V$ is an orthonormal matrix, whose columns are called right singular vectors.
 
For large matrices, usually we don't need the complete factorization but only the top singular
values and its associated singular vectors.  This can save storage, de-noise
and recover the low-rank structure of the matrix.

If we keep the top $k$ singular values, then the dimensions of the resulting low-rank matrix will be:

* `$U$`: `$m \times k$`,
* `$\Sigma$`: `$k \times k$`,
* `$V$`: `$n \times k$`.
 
### Performance
We assume $n$ is smaller than $m$. The singular values and the right singular vectors are derived
from the eigenvalues and the eigenvectors of the Gramian matrix $A^T A$. The matrix
storing the left singular vectors $U$, is computed via matrix multiplication as
$U = A (V S^{-1})$, if requested by the user via the computeU parameter. 
The actual method to use is determined automatically based on the computational cost:

* If $n$ is small ($n < 100$) or $k$ is large compared with $n$ ($k > n / 2$), we compute the Gramian matrix
first and then compute its top eigenvalues and eigenvectors locally on the driver.
This requires a single pass with $O(n^2)$ storage on each executor and on the driver, and
$O(n^2 k)$ time on the driver.
* Otherwise, we compute $(A^T A) v$ in a distributive way and send it to
<a href="http://www.caam.rice.edu/software/ARPACK/">ARPACK</a> to
compute $(A^T A)$'s top eigenvalues and eigenvectors on the driver node. This requires $O(k)$
passes, $O(n)$ storage on each executor, and $O(n k)$ storage on the driver.

### SVD Example
 
`spark.mllib` provides SVD functionality to row-oriented matrices, provided in the
<a href="mllib-data-types.html#rowmatrix">RowMatrix</a> class. 

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`SingularValueDecomposition` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.SingularValueDecomposition) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/SVDExample.scala %}

The same code applies to `IndexedRowMatrix` if `U` is defined as an
`IndexedRowMatrix`.
</div>
<div data-lang="java" markdown="1">
Refer to the [`SingularValueDecomposition` Java docs](api/java/org/apache/spark/mllib/linalg/SingularValueDecomposition.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaSVDExample.java %}

The same code applies to `IndexedRowMatrix` if `U` is defined as an
`IndexedRowMatrix`.

In order to run the above application, follow the instructions
provided in the [Self-Contained
Applications](quick-start.html#self-contained-applications) section of the Spark
quick-start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.

</div>
</div>

## Principal component analysis (PCA)

[Principal component analysis (PCA)](http://en.wikipedia.org/wiki/Principal_component_analysis) is a
statistical method to find a rotation such that the first coordinate has the largest variance
possible, and each succeeding coordinate in turn has the largest variance possible. The columns of
the rotation matrix are called principal components. PCA is used widely in dimensionality reduction.

`spark.mllib` supports PCA for tall-and-skinny matrices stored in row-oriented format and any Vectors.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The following code demonstrates how to compute principal components on a `RowMatrix`
and use them to project the vectors into a low-dimensional space.

Refer to the [`RowMatrix` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/PCAOnRowMatrixExample.scala %}

The following code demonstrates how to compute principal components on source vectors
and use them to project the vectors into a low-dimensional space while keeping associated labels:

Refer to the [`PCA` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.PCA) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/PCAOnSourceVectorExample.scala %}

</div>

<div data-lang="java" markdown="1">

The following code demonstrates how to compute principal components on a `RowMatrix`
and use them to project the vectors into a low-dimensional space.
The number of columns should be small, e.g, less than 1000.

Refer to the [`RowMatrix` Java docs](api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaPCAExample.java %}

</div>
</div>

In order to run the above application, follow the instructions
provided in the [Self-Contained Applications](quick-start.html#self-contained-applications)
section of the Spark
quick-start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.
