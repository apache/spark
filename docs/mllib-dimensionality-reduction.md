---
layout: global
title: Dimensionality Reduction - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Dimensionality Reduction
---

* Table of contents
{:toc}

[Dimensionality reduction](http://en.wikipedia.org/wiki/Dimensionality_reduction) is the process 
of reducing the number of variables under consideration.
It is used to extract latent features from raw and noisy features,
or compress data while maintaining the structure.
In this release, we provide preliminary support for dimensionality reduction on tall-and-skinny matrices.

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
values and its associated singular vectors.  This can save storage, and more importantly, de-noise
and recover the low-rank structure of the matrix.

If we keep the top $k$ singular values, then the dimensions of the return will be:

* `$U$`: `$m \times k$`,
* `$\Sigma$`: `$k \times k$`,
* `$V$`: `$n \times k$`.
 
In this release, we provide SVD computation to row-oriented matrices that have only a few columns,
say, less than $1000$, but many rows, which we call *tall-and-skinny*.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition

val mat: RowMatrix = ...

// Compute the top 20 singular values and corresponding singular vectors.
val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(20, computeU = true)
val U: RowMatrix = svd.U // The U factor is a RowMatrix.
val s: Vector = svd.s // The singular values are stored in a local dense vector.
val V: Matrix = svd.V // The V factor is a local dense matrix.
{% endhighlight %}
</div>
Same code applies to `IndexedRowMatrix`.
The only difference that the `U` matrix becomes an `IndexedRowMatrix`.
</div>

## Principal component analysis (PCA)

[Principal component analysis (PCA)](http://en.wikipedia.org/wiki/Principal_component_analysis) is a
statistical method to find a rotation such that the first coordinate has the largest variance
possible, and each succeeding coordinate in turn has the largest variance possible. The columns of
the rotation matrix are called principal components. PCA is used widely in dimensionality reduction.

In this release, we implement PCA for tall-and-skinny matrices stored in row-oriented format.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The following code demonstrates how to compute principal components on a tall-and-skinny `RowMatrix`
and use them to project the vectors into a low-dimensional space.
The number of columns should be small, e.g, less than 1000.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val mat: RowMatrix = ...

// Compute the top 10 principal components.
val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.

// Project the rows to the linear space spanned by the top 10 principal components.
val projected: RowMatrix = mat.multiply(pc)
{% endhighlight %}
</div>
</div>
