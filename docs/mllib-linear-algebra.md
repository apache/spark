---
layout: global
title: MLlib - Linear Algebra
---

Linear algebra is a fundamental building block for machine learning algorithms.
MLlib supports local vectors and matrices stored on a single machine, 
as well as distributed matrices backed by one or more RDDs.
In the current implementation, local vectors and matrices are simple data models 
to serve public interfaces. The underly linear algebra operations are provided by
[Breeze](http://www.scalanlp.org/) and [jblas](http://jblas.org/).

## Local vector

A local vector has integer-typed and 0-based indices and double-typed values, 
stored on a single machine.
MLlib supports two types of local vectors: dense and sparse.
A dense vector is backed by a double array representing its entry values, 
while a sparse vector is backed by two parallel arrays: indices and values.
For example, a vector $(1.0, 0.0, 3.0)$ can be represented in dense format as `[1.0, 0.0, 3.0]` or in sparse format as `(3, [0, 2], [1.0, 3.0])`, where `3` is the size of the vector.


<div class="codetabs">
<div data-lang="scala" markdown="1">
The base class of local vectors is [`Vector`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector),
and we provide two implementations: [`DenseVector`](api/mllib/index.html#org.apache.spark.mllib.linalg.DenseVector) and [`SparseVector`](api/mllib/index.html#org.apache.spark.mllib.linalg.SparseVector). 
We recommend using the factory methods implemented in [`Vectors`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector) to create local vectors.

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Create a dense vector (1.0, 0.0, 3.0).
val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
{% endhighlight %}

*Note*. Scala imports `scala.collection.immutable.Vector` by default, so you have to import `org.apache.spark.mllib.linalg.Vector` explicitly to use MLlib's `Vector`.
</div>

<div data-lang="java" markdown="1">
The base class of local vectors is [`Vector`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector),
and we provide two implementations: [`DenseVector`](api/mllib/index.html#org.apache.spark.mllib.linalg.DenseVector) and [`SparseVector`](api/mllib/index.html#org.apache.spark.mllib.linalg.SparseVector). 
We recommend using the factory methods implemented in [`Vectors`](api/mllib/index.html#org.apache.spark.mllib.linalg.Vector) to create local vectors.

{% highlight java %}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

// Create a dense vector (1.0, 0.0, 3.0).
Vector dv = Vectors.dense(1.0, 0.0, 3.0);
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
{% endhighlight %}
</div>

</div>

## Local matrix

A local matrix has integer-typed row and column indices and double-typed values, stored on a single machine.
MLlib supports dense matrix, whose entry values are stored in a single double array in column major.
For example, the following matrix
`\[
\begin{pmatrix}
1.0 & 2.0 \\
3.0 & 4.0 \\
5.0 & 6.0
\end{pmatrix}
\]`
is stored in a one-dimensional array `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]` with the matrix size `(3, 2)`.
We are going to add sparse matrix in the next release.

<div class="codetabs">
<div data-lang="scala" markdown="1">
The base class of local matrices is [`Matrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.Matrix),
and we provide one implementation: [`DenseMatrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.DenseMatrix).
Sparse matrix will be added in the next release.
We recommend using the factory methods implemented in [`Matrices`](api/mllib/index.html#org.apache.spark.mllib.linalg.Matrices) to create local matrices.

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
The base class of local matrices is [`Matrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.Matrix),
and we provide one implementation: [`DenseMatrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.DenseMatrix).
Sparse matrix will be added in the next release.
We recommend using the factory methods implemented in [`Matrices`](api/mllib/index.html#org.apache.spark.mllib.linalg.Matrices) to create local matrices.

{% highlight java %}
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;

// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
{% endhighlight %}
</div>

</div>

## Distributed matrix

A distributed matrix has long-typed row and column indices and double-typed values, stored distributively in one or more RDDs.
It is very important to choose the right format to store large and distributed matrices.
Converting a distributed matrix to a different format may require a global shuffle, which is quite expensive.
We implemented three types of distributed matrices in this release and will add more types in the future.

*Note*. The underlying RDDs of a distributed matrix must be deterministic, because we cache the matrix size.
It is always error-prone to have non-deterministic RDDs.

### RowMatrix

A `RowMatrix` is a row-oriented distributed matrix without meaningful row indices, backed by an RDD of its rows, where each row is a local vector.
This is similar to `data matrix` in the context of multivariate statistics.
Since each row is represented by a local vector, the number of columns is limited by the integer range but it should be much smaller in practice.

<div class="codetabs">
<div data-lang="scala" markdown="1">
A [`RowMatrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) can be created from an `RDD[Vector]` instance.
Then we can compute its column summary statistics.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val rows: RDD[Vector] = ... // an RDD of local vectors
// Create a RowMatrix from an RDD[Vector]
val mat: RowMatrix = new RowMatrix(rows)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

val summary: MultivariateStatisticalSummary = mat.computeColumnSummaryStatistics()
println(summary.mean)
println(summary.variance)
println(summary.numNonzers)
{% endhighlight %}
</div>
</div>

If a matrix has only a few columns, say, less than $1000$, but many rows, we call it tall-and-skinny.
Many algorithms can be implemented efficiently on tall-and-skinny matrices,
and we provide methods to compute [Gramian matrix](http://en.wikipedia.org/wiki/Gramian_matrix), [covariance matrix](http://en.wikipedia.org/wiki/Covariance_matrix),
[singular value decomposition](http://en.wikipedia.org/wiki/Singular_value_decomposition), and [principal components](http://en.wikipedia.org/wiki/Principal_component_analysis). We also support multiplying a `RowMatrix` with a local matrix, which is used frequently in dimensionality reduction. 

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
val mat: RowMatrix = ...

val G: Matrix = mat.computeGramianMatrix() // The Gramian matrix is a local dense matrix.

val Cov: Matrix = mat.computeCovariance() // The covariance matrix is a local dense matrix.

// Compute the top 10 singular values and corresponding singular vectors.
val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(20, computeU = true)
val U: RowMatrix = svd.U // The U factor is a RowMatrix.
val s: Vector = svd.s // The singular values are stored in a local dense vector.
val V: Matrix = svd.V // The V factor is a local dense matrix.

// Compute the top 10 principal components.
val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.

// Project the rows to the linear space spanned by the top 10 principal components.
val projected: RowMatrix = mat.multiply(pc)
{% endhighlight %}
</div>
</div>

### IndexedRowMatrix

An `IndexedRowMatrix` is similar to a `RowMatrix` but with meaningful row indices.
It is backed by an RDD of indexed rows, which each row is represented by its index (long-typed) and a local vector.

<div class="codetabs">
<div data-lang="scala" markdown="1">
An [`IndexedRowMatrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix) can be created from an `RDD[IndexedRow]` instance, 
where [`IndexedRow`](api/mllib/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRow) is a wrapper over `(Long, Vector)`.
An `IndexedRowMatrix` can be converted to a `RowMatrix` by dropping its row indices.
In this release, we only provide algorithms for tall-and-skinny `IndexedRowMatrix`, including `computeGramianMatrix`, `computeSVD`, and `multiply`.

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

val rows: RDD[IndexedRow] = ... // an RDD of indexed rows
// Create an IndexedRowMatrix from an RDD[IndexedRow].
val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

// Compute the top 10 singular values and corresponding singular vectors.
val svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = mat.computeSVD(20, computeU = true)
val U: IndexedRowMatrix = svd.U // The U factor is an IndexedRowMatrix.
val s: Vector = svd.s // The singular values are stored in a local dense vector.
val V: Matrix = svd.V // The V factor is a local dense matrix.
{% endhighlight %}
</div>
</div>

### CoordinateMatrix

A `CoordinateMatrix` is a distributed matrix backed by an RDD of its entries.
Each entry is a tuple of `(i: Long, j: Long, value: Double)`, where `i` is the row index, `j` is the column index, and `value` is the entry value.
A `CoordinateMatrix` should be used only in the case when both dimensions of the matrix are huge and the matrix is very sparse.

<div class="codetabs">
<div data-lang="scala" markdown="1">
A [`CoordinateMatrix`](api/mllib/index.html#org.apache.spark.mllib.linalg.distributed.CoordinateMatrix) can be created from an `RDD[MatrixEntry]` instance, 
where [`MatrixEntry`](api/mllib/index.html#org.apache.spark.mllib.linalg.distributed.MatrixEntry) is a wrapper over `(Long, Long, Double)`.
A `CoordinateMatrix` can be converted to a `IndexedRowMatrix` with sparse rows by calling `toIndexedRowMatrix`.
In this release, we do not provide other computation for `CoordinateMatrix`.

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

val entries: RDD[MatrixEntry] = ... // an RDD of matrix entries
// Create a CoordinateMatrix from an RDD[MatrixEntry].
val mat: CoordinateMatrix = new CoordinateMatrix(entries)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

// Convert it to an IndexRowMatrix whose rows are sparse vectors.
val indexedRowMatrix = mat.toIndexedRowMatrix()
{% endhighlight %}
</div>
</div>