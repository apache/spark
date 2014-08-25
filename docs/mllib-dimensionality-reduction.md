---
layout: global
title: Dimensionality Reduction - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Dimensionality Reduction
---

* Table of contents
{:toc}

[Dimensionality reduction](http://en.wikipedia.org/wiki/Dimensionality_reduction) is the process 
of reducing the number of variables under consideration.
It can be used to extract latent features from raw and noisy features
or compress data while maintaining the structure.
MLlib provides support for dimensionality reduction on the <a href="mllib-basics.html#rowmatrix">RowMatrix</a> class.

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
 
MLlib provides SVD functionality to row-oriented matrices, provided in the
<a href="mllib-basics.html#rowmatrix">RowMatrix</a> class. 

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

The same code applies to `IndexedRowMatrix` if `U` is defined as an
`IndexedRowMatrix`.
</div>
<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.LinkedList;

import org.apache.spark.api.java.*;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SVD {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SVD Example");
    SparkContext sc = new SparkContext(conf);
     
    double[][] array = ...
    LinkedList<Vector> rowsList = new LinkedList<Vector>();
    for (int i = 0; i < array.length; i++) {
      Vector currentRow = Vectors.dense(array[i]);
      rowsList.add(currentRow);
    }
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);

    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Compute the top 4 singular values and corresponding singular vectors.
    SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(4, true, 1.0E-9d);
    RowMatrix U = svd.U();
    Vector s = svd.s();
    Matrix V = svd.V();
  }
}
{% endhighlight %}

The same code applies to `IndexedRowMatrix` if `U` is defined as an
`IndexedRowMatrix`.

In order to run the above standalone application, follow the instructions
provided in the [Standalone
Applications](quick-start.html#standalone-applications) section of the Spark
quick-start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.

</div>
</div>

## Principal component analysis (PCA)

[Principal component analysis (PCA)](http://en.wikipedia.org/wiki/Principal_component_analysis) is a
statistical method to find a rotation such that the first coordinate has the largest variance
possible, and each succeeding coordinate in turn has the largest variance possible. The columns of
the rotation matrix are called principal components. PCA is used widely in dimensionality reduction.

MLlib supports PCA for tall-and-skinny matrices stored in row-oriented format.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The following code demonstrates how to compute principal components on a `RowMatrix`
and use them to project the vectors into a low-dimensional space.

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

<div data-lang="java" markdown="1">

The following code demonstrates how to compute principal components on a `RowMatrix`
and use them to project the vectors into a low-dimensional space.
The number of columns should be small, e.g, less than 1000.

{% highlight java %}
import java.util.LinkedList;

import org.apache.spark.api.java.*;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class PCA {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("PCA Example");
    SparkContext sc = new SparkContext(conf);
     
    double[][] array = ...
    LinkedList<Vector> rowsList = new LinkedList<Vector>();
    for (int i = 0; i < array.length; i++) {
      Vector currentRow = Vectors.dense(array[i]);
      rowsList.add(currentRow);
    }
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);

    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Compute the top 3 principal components.
    Matrix pc = mat.computePrincipalComponents(3);
    RowMatrix projected = mat.multiply(pc);
  }
}
{% endhighlight %}

In order to run the above standalone application, follow the instructions
provided in the [Standalone
Applications](quick-start.html#standalone-applications) section of the Spark
quick-start guide. Be sure to also include *spark-mllib* to your build file as
a dependency.
</div>
</div>
