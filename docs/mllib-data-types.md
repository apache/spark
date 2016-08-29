---
layout: global
title: Data Types - RDD-based API
displayTitle: Data Types - RDD-based API
---

* Table of contents
{:toc}

MLlib supports local vectors and matrices stored on a single machine, 
as well as distributed matrices backed by one or more RDDs.
Local vectors and local matrices are simple data models 
that serve as public interfaces. The underlying linear algebra operations are provided by
[Breeze](http://www.scalanlp.org/).
A training example used in supervised learning is called a "labeled point" in MLlib.

## Local vector

A local vector has integer-typed and 0-based indices and double-typed values, stored on a single
machine.  MLlib supports two types of local vectors: dense and sparse.  A dense vector is backed by
a double array representing its entry values, while a sparse vector is backed by two parallel
arrays: indices and values.  For example, a vector `(1.0, 0.0, 3.0)` can be represented in dense
format as `[1.0, 0.0, 3.0]` or in sparse format as `(3, [0, 2], [1.0, 3.0])`, where `3` is the size
of the vector.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The base class of local vectors is
[`Vector`](api/scala/index.html#org.apache.spark.mllib.linalg.Vector), and we provide two
implementations: [`DenseVector`](api/scala/index.html#org.apache.spark.mllib.linalg.DenseVector) and
[`SparseVector`](api/scala/index.html#org.apache.spark.mllib.linalg.SparseVector).  We recommend
using the factory methods implemented in
[`Vectors`](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) to create local vectors.

Refer to the [`Vector` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.Vector) and [`Vectors` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Create a dense vector (1.0, 0.0, 3.0).
val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
{% endhighlight %}

***Note:***
Scala imports `scala.collection.immutable.Vector` by default, so you have to import
`org.apache.spark.mllib.linalg.Vector` explicitly to use MLlib's `Vector`.

</div>

<div data-lang="java" markdown="1">

The base class of local vectors is
[`Vector`](api/java/org/apache/spark/mllib/linalg/Vector.html), and we provide two
implementations: [`DenseVector`](api/java/org/apache/spark/mllib/linalg/DenseVector.html) and
[`SparseVector`](api/java/org/apache/spark/mllib/linalg/SparseVector.html).  We recommend
using the factory methods implemented in
[`Vectors`](api/java/org/apache/spark/mllib/linalg/Vectors.html) to create local vectors.

Refer to the [`Vector` Java docs](api/java/org/apache/spark/mllib/linalg/Vector.html) and [`Vectors` Java docs](api/java/org/apache/spark/mllib/linalg/Vectors.html) for details on the API.

{% highlight java %}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

// Create a dense vector (1.0, 0.0, 3.0).
Vector dv = Vectors.dense(1.0, 0.0, 3.0);
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
MLlib recognizes the following types as dense vectors:

* NumPy's [`array`](http://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html)
* Python's list, e.g., `[1, 2, 3]`

and the following as sparse vectors:

* MLlib's [`SparseVector`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.SparseVector).
* SciPy's
  [`csc_matrix`](http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csc_matrix.html#scipy.sparse.csc_matrix)
  with a single column

We recommend using NumPy arrays over lists for efficiency, and using the factory methods implemented
in [`Vectors`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.Vectors) to create sparse vectors.

Refer to the [`Vectors` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.Vectors) for more details on the API.

{% highlight python %}
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors

# Use a NumPy array as a dense vector.
dv1 = np.array([1.0, 0.0, 3.0])
# Use a Python list as a dense vector.
dv2 = [1.0, 0.0, 3.0]
# Create a SparseVector.
sv1 = Vectors.sparse(3, [0, 2], [1.0, 3.0])
# Use a single-column SciPy csc_matrix as a sparse vector.
sv2 = sps.csc_matrix((np.array([1.0, 3.0]), np.array([0, 2]), np.array([0, 2])), shape = (3, 1))
{% endhighlight %}

</div>
</div>

## Labeled point

A labeled point is a local vector, either dense or sparse, associated with a label/response.
In MLlib, labeled points are used in supervised learning algorithms.
We use a double to store a label, so we can use labeled points in both regression and classification.
For binary classification, a label should be either `0` (negative) or `1` (positive).
For multiclass classification, labels should be class indices starting from zero: `0, 1, 2, ...`.

<div class="codetabs">

<div data-lang="scala" markdown="1">

A labeled point is represented by the case class
[`LabeledPoint`](api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint).

Refer to the [`LabeledPoint` Scala docs](api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

// Create a labeled point with a positive label and a dense feature vector.
val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

// Create a labeled point with a negative label and a sparse feature vector.
val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

A labeled point is represented by
[`LabeledPoint`](api/java/org/apache/spark/mllib/regression/LabeledPoint.html).

Refer to the [`LabeledPoint` Java docs](api/java/org/apache/spark/mllib/regression/LabeledPoint.html) for details on the API.

{% highlight java %}
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

// Create a labeled point with a positive label and a dense feature vector.
LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

// Create a labeled point with a negative label and a sparse feature vector.
LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

A labeled point is represented by
[`LabeledPoint`](api/python/pyspark.mllib.html#pyspark.mllib.regression.LabeledPoint).

Refer to the [`LabeledPoint` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.regression.LabeledPoint) for more details on the API.

{% highlight python %}
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Create a labeled point with a positive label and a dense feature vector.
pos = LabeledPoint(1.0, [1.0, 0.0, 3.0])

# Create a labeled point with a negative label and a sparse feature vector.
neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
{% endhighlight %}
</div>
</div>

***Sparse data***

It is very common in practice to have sparse training data.  MLlib supports reading training
examples stored in `LIBSVM` format, which is the default format used by
[`LIBSVM`](http://www.csie.ntu.edu.tw/~cjlin/libsvm/) and
[`LIBLINEAR`](http://www.csie.ntu.edu.tw/~cjlin/liblinear/).  It is a text format in which each line
represents a labeled sparse feature vector using the following format:

~~~
label index1:value1 index2:value2 ...
~~~

where the indices are one-based and in ascending order. 
After loading, the feature indices are converted to zero-based.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`MLUtils.loadLibSVMFile`](api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) reads training
examples stored in LIBSVM format.

Refer to the [`MLUtils` Scala docs](api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`MLUtils.loadLibSVMFile`](api/java/org/apache/spark/mllib/util/MLUtils.html) reads training
examples stored in LIBSVM format.

Refer to the [`MLUtils` Java docs](api/java/org/apache/spark/mllib/util/MLUtils.html) for details on the API.

{% highlight java %}
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.JavaRDD;

JavaRDD<LabeledPoint> examples = 
  MLUtils.loadLibSVMFile(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`MLUtils.loadLibSVMFile`](api/python/pyspark.mllib.html#pyspark.mllib.util.MLUtils) reads training
examples stored in LIBSVM format.

Refer to the [`MLUtils` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.util.MLUtils) for more details on the API.

{% highlight python %}
from pyspark.mllib.util import MLUtils

examples = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
{% endhighlight %}
</div>
</div>

## Local matrix

A local matrix has integer-typed row and column indices and double-typed values, stored on a single
machine.  MLlib supports dense matrices, whose entry values are stored in a single double array in
column-major order, and sparse matrices, whose non-zero entry values are stored in the Compressed Sparse
Column (CSC) format in column-major order.  For example, the following dense matrix `\[ \begin{pmatrix}
1.0 & 2.0 \\
3.0 & 4.0 \\
5.0 & 6.0
\end{pmatrix}
\]`
is stored in a one-dimensional array `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]` with the matrix size `(3, 2)`.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The base class of local matrices is
[`Matrix`](api/scala/index.html#org.apache.spark.mllib.linalg.Matrix), and we provide two
implementations: [`DenseMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.DenseMatrix),
and [`SparseMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.SparseMatrix).
We recommend using the factory methods implemented
in [`Matrices`](api/scala/index.html#org.apache.spark.mllib.linalg.Matrices$) to create local
matrices. Remember, local matrices in MLlib are stored in column-major order.

Refer to the [`Matrix` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.Matrix) and [`Matrices` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.Matrices$) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

// Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

The base class of local matrices is
[`Matrix`](api/java/org/apache/spark/mllib/linalg/Matrix.html), and we provide two
implementations: [`DenseMatrix`](api/java/org/apache/spark/mllib/linalg/DenseMatrix.html),
and [`SparseMatrix`](api/java/org/apache/spark/mllib/linalg/SparseMatrix.html).
We recommend using the factory methods implemented
in [`Matrices`](api/java/org/apache/spark/mllib/linalg/Matrices.html) to create local
matrices. Remember, local matrices in MLlib are stored in column-major order.

Refer to the [`Matrix` Java docs](api/java/org/apache/spark/mllib/linalg/Matrix.html) and [`Matrices` Java docs](api/java/org/apache/spark/mllib/linalg/Matrices.html) for details on the API.

{% highlight java %}
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;

// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

// Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

The base class of local matrices is
[`Matrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.Matrix), and we provide two
implementations: [`DenseMatrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.DenseMatrix),
and [`SparseMatrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.SparseMatrix).
We recommend using the factory methods implemented
in [`Matrices`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.Matrices) to create local
matrices. Remember, local matrices in MLlib are stored in column-major order.

Refer to the [`Matrix` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.Matrix) and [`Matrices` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.Matrices) for more details on the API.

{% highlight python %}
from pyspark.mllib.linalg import Matrix, Matrices

# Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
dm2 = Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])

# Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
sm = Matrices.sparse(3, 2, [0, 1, 3], [0, 2, 1], [9, 6, 8])
{% endhighlight %}
</div>

</div>

## Distributed matrix

A distributed matrix has long-typed row and column indices and double-typed values, stored
distributively in one or more RDDs.  It is very important to choose the right format to store large
and distributed matrices.  Converting a distributed matrix to a different format may require a
global shuffle, which is quite expensive. Four types of distributed matrices have been implemented
so far.

The basic type is called `RowMatrix`. A `RowMatrix` is a row-oriented distributed
matrix without meaningful row indices, e.g., a collection of feature vectors.
It is backed by an RDD of its rows, where each row is a local vector.
We assume that the number of columns is not huge for a `RowMatrix` so that a single
local vector can be reasonably communicated to the driver and can also be stored /
operated on using a single node. 
An `IndexedRowMatrix` is similar to a `RowMatrix` but with row indices,
which can be used for identifying rows and executing joins.
A `CoordinateMatrix` is a distributed matrix stored in [coordinate list (COO)](https://en.wikipedia.org/wiki/Sparse_matrix#Coordinate_list_.28COO.29) format,
backed by an RDD of its entries.
A `BlockMatrix` is a distributed matrix backed by an RDD of `MatrixBlock`
which is a tuple of `(Int, Int, Matrix)`.

***Note***

The underlying RDDs of a distributed matrix must be deterministic, because we cache the matrix size.
In general the use of non-deterministic RDDs can lead to errors.

### RowMatrix

A `RowMatrix` is a row-oriented distributed matrix without meaningful row indices, backed by an RDD
of its rows, where each row is a local vector.
Since each row is represented by a local vector, the number of columns is
limited by the integer range but it should be much smaller in practice.

<div class="codetabs">
<div data-lang="scala" markdown="1">

A [`RowMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) can be
created from an `RDD[Vector]` instance.  Then we can compute its column summary statistics and decompositions.
[QR decomposition](https://en.wikipedia.org/wiki/QR_decomposition) is of the form A = QR where Q is an orthogonal matrix and R is an upper triangular matrix.
For [singular value decomposition (SVD)](https://en.wikipedia.org/wiki/Singular_value_decomposition) and [principal component analysis (PCA)](https://en.wikipedia.org/wiki/Principal_component_analysis), please refer to [Dimensionality reduction](mllib-dimensionality-reduction.html).

Refer to the [`RowMatrix` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val rows: RDD[Vector] = ... // an RDD of local vectors
// Create a RowMatrix from an RDD[Vector].
val mat: RowMatrix = new RowMatrix(rows)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

// QR decomposition 
val qrResult = mat.tallSkinnyQR(true)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

A [`RowMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html) can be
created from a `JavaRDD<Vector>` instance.  Then we can compute its column summary statistics.

Refer to the [`RowMatrix` Java docs](api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

JavaRDD<Vector> rows = ... // a JavaRDD of local vectors
// Create a RowMatrix from an JavaRDD<Vector>.
RowMatrix mat = new RowMatrix(rows.rdd());

// Get its size.
long m = mat.numRows();
long n = mat.numCols();

// QR decomposition 
QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

A [`RowMatrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.RowMatrix) can be 
created from an `RDD` of vectors.

Refer to the [`RowMatrix` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.RowMatrix) for more details on the API.

{% highlight python %}
from pyspark.mllib.linalg.distributed import RowMatrix

# Create an RDD of vectors.
rows = sc.parallelize([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])

# Create a RowMatrix from an RDD of vectors.
mat = RowMatrix(rows)

# Get its size.
m = mat.numRows()  # 4
n = mat.numCols()  # 3

# Get the rows as an RDD of vectors again.
rowsRDD = mat.rows
{% endhighlight %}
</div>

</div>

### IndexedRowMatrix

An `IndexedRowMatrix` is similar to a `RowMatrix` but with meaningful row indices.  It is backed by
an RDD of indexed rows, so that each row is represented by its index (long-typed) and a local 
vector.

<div class="codetabs">
<div data-lang="scala" markdown="1">

An
[`IndexedRowMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix)
can be created from an `RDD[IndexedRow]` instance, where
[`IndexedRow`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRow) is a
wrapper over `(Long, Vector)`.  An `IndexedRowMatrix` can be converted to a `RowMatrix` by dropping
its row indices.

Refer to the [`IndexedRowMatrix` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

val rows: RDD[IndexedRow] = ... // an RDD of indexed rows
// Create an IndexedRowMatrix from an RDD[IndexedRow].
val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

// Drop its row indices.
val rowMat: RowMatrix = mat.toRowMatrix()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

An
[`IndexedRowMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/IndexedRowMatrix.html)
can be created from an `JavaRDD<IndexedRow>` instance, where
[`IndexedRow`](api/java/org/apache/spark/mllib/linalg/distributed/IndexedRow.html) is a
wrapper over `(long, Vector)`.  An `IndexedRowMatrix` can be converted to a `RowMatrix` by dropping
its row indices.

Refer to the [`IndexedRowMatrix` Java docs](api/java/org/apache/spark/mllib/linalg/distributed/IndexedRowMatrix.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

JavaRDD<IndexedRow> rows = ... // a JavaRDD of indexed rows
// Create an IndexedRowMatrix from a JavaRDD<IndexedRow>.
IndexedRowMatrix mat = new IndexedRowMatrix(rows.rdd());

// Get its size.
long m = mat.numRows();
long n = mat.numCols();

// Drop its row indices.
RowMatrix rowMat = mat.toRowMatrix();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

An [`IndexedRowMatrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.IndexedRowMatrix)
can be created from an `RDD` of `IndexedRow`s, where 
[`IndexedRow`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.IndexedRow) is a 
wrapper over `(long, vector)`.  An `IndexedRowMatrix` can be converted to a `RowMatrix` by dropping
its row indices.

Refer to the [`IndexedRowMatrix` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.IndexedRowMatrix) for more details on the API.

{% highlight python %}
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix

# Create an RDD of indexed rows.
#   - This can be done explicitly with the IndexedRow class:
indexedRows = sc.parallelize([IndexedRow(0, [1, 2, 3]), 
                              IndexedRow(1, [4, 5, 6]), 
                              IndexedRow(2, [7, 8, 9]), 
                              IndexedRow(3, [10, 11, 12])])
#   - or by using (long, vector) tuples:
indexedRows = sc.parallelize([(0, [1, 2, 3]), (1, [4, 5, 6]), 
                              (2, [7, 8, 9]), (3, [10, 11, 12])])

# Create an IndexedRowMatrix from an RDD of IndexedRows.
mat = IndexedRowMatrix(indexedRows)

# Get its size.
m = mat.numRows()  # 4
n = mat.numCols()  # 3

# Get the rows as an RDD of IndexedRows.
rowsRDD = mat.rows

# Convert to a RowMatrix by dropping the row indices.
rowMat = mat.toRowMatrix()
{% endhighlight %}
</div>

</div>

### CoordinateMatrix

A `CoordinateMatrix` is a distributed matrix backed by an RDD of its entries.  Each entry is a tuple
of `(i: Long, j: Long, value: Double)`, where `i` is the row index, `j` is the column index, and
`value` is the entry value.  A `CoordinateMatrix` should be used only when both
dimensions of the matrix are huge and the matrix is very sparse.

<div class="codetabs">
<div data-lang="scala" markdown="1">

A
[`CoordinateMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.CoordinateMatrix)
can be created from an `RDD[MatrixEntry]` instance, where
[`MatrixEntry`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.MatrixEntry) is a
wrapper over `(Long, Long, Double)`.  A `CoordinateMatrix` can be converted to an `IndexedRowMatrix`
with sparse rows by calling `toIndexedRowMatrix`.  Other computations for 
`CoordinateMatrix` are not currently supported.

Refer to the [`CoordinateMatrix` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.CoordinateMatrix) for details on the API.

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

<div data-lang="java" markdown="1">

A
[`CoordinateMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/CoordinateMatrix.html)
can be created from a `JavaRDD<MatrixEntry>` instance, where
[`MatrixEntry`](api/java/org/apache/spark/mllib/linalg/distributed/MatrixEntry.html) is a
wrapper over `(long, long, double)`.  A `CoordinateMatrix` can be converted to an `IndexedRowMatrix`
with sparse rows by calling `toIndexedRowMatrix`. Other computations for 
`CoordinateMatrix` are not currently supported.

Refer to the [`CoordinateMatrix` Java docs](api/java/org/apache/spark/mllib/linalg/distributed/CoordinateMatrix.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

JavaRDD<MatrixEntry> entries = ... // a JavaRDD of matrix entries
// Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());

// Get its size.
long m = mat.numRows();
long n = mat.numCols();

// Convert it to an IndexRowMatrix whose rows are sparse vectors.
IndexedRowMatrix indexedRowMatrix = mat.toIndexedRowMatrix();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

A [`CoordinateMatrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.CoordinateMatrix)
can be created from an `RDD` of `MatrixEntry` entries, where 
[`MatrixEntry`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.MatrixEntry) is a 
wrapper over `(long, long, float)`.  A `CoordinateMatrix` can be converted to a `RowMatrix` by 
calling `toRowMatrix`, or to an `IndexedRowMatrix` with sparse rows by calling `toIndexedRowMatrix`.

Refer to the [`CoordinateMatrix` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.CoordinateMatrix) for more details on the API.

{% highlight python %}
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry

# Create an RDD of coordinate entries.
#   - This can be done explicitly with the MatrixEntry class:
entries = sc.parallelize([MatrixEntry(0, 0, 1.2), MatrixEntry(1, 0, 2.1), MatrixEntry(6, 1, 3.7)])
#   - or using (long, long, float) tuples:
entries = sc.parallelize([(0, 0, 1.2), (1, 0, 2.1), (2, 1, 3.7)])

# Create an CoordinateMatrix from an RDD of MatrixEntries.
mat = CoordinateMatrix(entries)

# Get its size.
m = mat.numRows()  # 3
n = mat.numCols()  # 2

# Get the entries as an RDD of MatrixEntries.
entriesRDD = mat.entries

# Convert to a RowMatrix.
rowMat = mat.toRowMatrix()

# Convert to an IndexedRowMatrix.
indexedRowMat = mat.toIndexedRowMatrix()

# Convert to a BlockMatrix.
blockMat = mat.toBlockMatrix()
{% endhighlight %}
</div>

</div>

### BlockMatrix

A `BlockMatrix` is a distributed matrix backed by an RDD of `MatrixBlock`s, where a `MatrixBlock` is
a tuple of `((Int, Int), Matrix)`, where the `(Int, Int)` is the index of the block, and `Matrix` is
the sub-matrix at the given index with size `rowsPerBlock` x `colsPerBlock`.
`BlockMatrix` supports methods such as `add` and `multiply` with another `BlockMatrix`.
`BlockMatrix` also has a helper function `validate` which can be used to check whether the
`BlockMatrix` is set up properly.

<div class="codetabs">
<div data-lang="scala" markdown="1">

A [`BlockMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) can be
most easily created from an `IndexedRowMatrix` or `CoordinateMatrix` by calling `toBlockMatrix`.
`toBlockMatrix` creates blocks of size 1024 x 1024 by default.
Users may change the block size by supplying the values through `toBlockMatrix(rowsPerBlock, colsPerBlock)`.

Refer to the [`BlockMatrix` Scala docs](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}

val entries: RDD[MatrixEntry] = ... // an RDD of (i, j, v) matrix entries
// Create a CoordinateMatrix from an RDD[MatrixEntry].
val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
// Transform the CoordinateMatrix to a BlockMatrix
val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

// Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
// Nothing happens if it is valid.
matA.validate()

// Calculate A^T A.
val ata = matA.transpose.multiply(matA)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

A [`BlockMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/BlockMatrix.html) can be
most easily created from an `IndexedRowMatrix` or `CoordinateMatrix` by calling `toBlockMatrix`.
`toBlockMatrix` creates blocks of size 1024 x 1024 by default.
Users may change the block size by supplying the values through `toBlockMatrix(rowsPerBlock, colsPerBlock)`.

Refer to the [`BlockMatrix` Java docs](api/java/org/apache/spark/mllib/linalg/distributed/BlockMatrix.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

JavaRDD<MatrixEntry> entries = ... // a JavaRDD of (i, j, v) Matrix Entries
// Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());
// Transform the CoordinateMatrix to a BlockMatrix
BlockMatrix matA = coordMat.toBlockMatrix().cache();

// Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
// Nothing happens if it is valid.
matA.validate();

// Calculate A^T A.
BlockMatrix ata = matA.transpose().multiply(matA);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

A [`BlockMatrix`](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.BlockMatrix) 
can be created from an `RDD` of sub-matrix blocks, where a sub-matrix block is a 
`((blockRowIndex, blockColIndex), sub-matrix)` tuple.

Refer to the [`BlockMatrix` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.linalg.distributed.BlockMatrix) for more details on the API.

{% highlight python %}
from pyspark.mllib.linalg import Matrices
from pyspark.mllib.linalg.distributed import BlockMatrix

# Create an RDD of sub-matrix blocks.
blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])), 
                         ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])

# Create a BlockMatrix from an RDD of sub-matrix blocks.
mat = BlockMatrix(blocks, 3, 2)

# Get its size.
m = mat.numRows() # 6
n = mat.numCols() # 2

# Get the blocks as an RDD of sub-matrix blocks.
blocksRDD = mat.blocks

# Convert to a LocalMatrix.
localMat = mat.toLocalMatrix()

# Convert to an IndexedRowMatrix.
indexedRowMat = mat.toIndexedRowMatrix()

# Convert to a CoordinateMatrix.
coordinateMat = mat.toCoordinateMatrix()
{% endhighlight %}
</div>
</div>
