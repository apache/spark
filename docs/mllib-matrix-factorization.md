---
layout: global
title: Matrix Factorization - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Matrix Factorization
---

* Table of contents
{:toc}

#LU Factorization

[`blockLU()`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) returns a [`BlockMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix)
The LU decomposition is a well studied algorithm for decomposing a matrix into a lower diagonal matrix $L$ and an upper diagonal matrix $U$.  

`\begin{equation}
    PA = LU\\
        P \begin{pmatrix}
            a_{11}&\cdots  &a_{1n} \\ 
             \vdots& \ddots &\vdots \\ 
             a_{m1}&\cdots  & a_{mn}
        \end{pmatrix} = 
        \begin{pmatrix}
            \ell_{11}&\ 0    &0 \\ 
             \vdots& \ddots &0 \\ 
             \ell_{m1}&\cdots  & \ell_{mn}
            \end{pmatrix}
            \begin{pmatrix}
               0&\ \cdots    &u_{1n} \\ 
             \vdots& \ddots &\vdots\\ 
            0&\cdots  & u_{mn}
        \end{pmatrix},
        \label{eq:generalLUFactorization}
\end{equation}`

and $P$ is a row permutation matrix.  This algorithm is a highly stable method for inverting a matrix and solving linear systems of equations that appear in machine learning applications.  Larger linear equations of the type $Ax=b$ are usually solved with SGD, BGFS, or other gradient based methods.  Being able to solve these equations at scale to numerical precision rather than to convergence should open up possiblities for new algorithms within MLlib as well as other applications.  It scales as ~$n^3$, for a [`BlockMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) with $n \times n$ blocks.

Once the decomposition is computed, the inverse is straightforward to compute using a forward subsitution method on $L$ and $U$.  The inversion method is not yet available in [`BlockMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix).


**Example**

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix}
import org.apache.spark.mllib.linalg.{Matrix=>SparkMatrix,Matrices=>SparkMatrices}
import org.apache.spark.mllib.linalg.{DenseMatrix}

val blocks: Seq[((Int, Int), SparkMatrix)] = Seq(
  ((0, 0), new DenseMatrix(2, 2, Array(1.0, 2.0, 3.0, 2.0))),
  ((0, 1), new DenseMatrix(2, 2, Array(2.0, 1.0, 3.0, 5.0))),
  ((1, 0), new DenseMatrix(2, 2, Array(3.0, 2.0, 1.0, 1.0))),
  ((1, 1), new DenseMatrix(2, 2, Array(1.0, 2.0, 0.0, 1.0))), 
  ((0, 2), new DenseMatrix(2, 1, Array(1.0, 1.0))),
  ((1, 2), new DenseMatrix(2, 1, Array(1.0, 3.0))),
  ((2, 0), new DenseMatrix(1, 2, Array(1.0, 0.0))),
  ((2, 1), new DenseMatrix(1, 2, Array(1.0, 2.0))),
  ((2, 2), new DenseMatrix(1, 1, Array(4.0))))

val rowsPerBlock = 2; val colsPerBlock = 2; 
 val A =  
       new BlockMatrix(sc.parallelize(blocks), rowsPerBlock, colsPerBlock)
    
val PLU = A.blockLU
val P  = PLU._1
val L  = PLU._2
val U  = PLU._3

// computing a fast residual...top and bottom matrices only
val residual = L.multiply(U).subtract(P.multiply(A))
val error = residual.toLocalMatrix.toArray.reduce(_ + Math.abs(_) ) 

println( "error (sb ~6.7e-16): " + error.toString )
{% endhighlight %}

**How it Works**

The LU decomposition of $A$ can be written in 4 block form as:
`\begin{align}
    PA & = LU\\
        \begin{pmatrix}
            P_1 A_{11}&P_1 A_{12} \\ 
            P_2 A_{21}&P_2 A_{22}
        \end{pmatrix} 
        & = \begin{pmatrix}
            L_{11}&0 \\ 
            L_{21}&L_{22}
            \end{pmatrix}
            \begin{pmatrix}
                U_{11}&U_{12} \\ 
                0&U_{22}
        \end{pmatrix} \\
        & =         \begin{pmatrix}
            L_{11}U_{11}&L_{11}U_{12} \\ 
            L_{21}U_{11}&L_{21}U_{12}+L_{22}U_{22}
            \end{pmatrix}
        \label{eq:basicLUBlockDecomposition}
\end{align}`

Once the blocks are defined, we can then solve each matrix quadrant individually.  

`\begin{align}
P_1 A_{11} & = L_{11}U_{11}               & \Rightarrow  & (P_{1},L_{11},U_{11})  & = & \text{LU}_{LOCAL}(A_{11}) \label{eq:A11Solve} \\
P_1 A_{12} & = L_{11}U_{12}               & \Rightarrow  & U_{12}               & = & L_{11}^{-1}P_1 A_{12} \label{eq:U12Solve} \\
P_2 A_{21} & = L_{21}U_{11}               & \Rightarrow  & L_{21}               & = &P_2 A_{21}U_{11}^{-1} \label{eq:L21Solve}\\
P_2 A_{22} & = L_{21}U_{12}+L_{22}U_{22}  & \Rightarrow  & (P_{2},L_{22},U_{22})  & = & \text{LU}_{RECURSIVE}(S) \label{eq:A22Solve}\\
\end{align}`

where $A_{11}$ is chosen to be a single block, so that the Breeze library can be called to compute $\eqref{eq:A11Solve}$.  The Breeze library will return the $P_{1},L_{11}$ and $U_{11}$ matrices.  Equation $\eqref{eq:A22Solve}$ is a recursive call that generates successive calculations of the Schur Complement, given as 
 
`\begin{equation}
S = A_{22} - L_{21} U_{12}.
\label{eq:SchurComplementInitialForm}
\end{equation}`

In this form, there is a dependency on the calculation of $\eqref{eq:A11Solve}$.  An equivalent form of the Schur complement can be used by substituting equations $\eqref{eq:U12Solve}$ and $\eqref{eq:L21Solve}$, giving

`\begin{equation}
S = A_{22} - A_{21} A_{11}^{-1} A_{12}, 
\label{eq:SchurComplementFinalForm}
\end{equation}`

which allows for the calculation of equation $\eqref{eq:A22Solve}$ with no dependency on equation $\eqref{eq:A11Solve}$, resulting in a slight increase in parallelism at the expense of recomputing the inverse of $A_{11}$ on a separate process.  

The Schur Complement in $\eqref{eq:A22Solve}$ is computed using $\eqref{eq:SchurComplementFinalForm}$ and passed recursively to the next itereration.  In this way, $P_2$ is never explicitly calculated, but is built up as a set of $P_1$ matrices.  The computation completes when the Schur Complement is a single block.


  Equations $\eqref{eq:U12Solve}$ and $\eqref{eq:L21Solve}$ are computed with [`BlockMatrix.multiply()`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) operations.  The Schur Complement in $\eqref{eq:A22Solve}$ is computed using $\eqref{eq:SchurComplementFinalForm}$ with a [`BlockMatrix.multiply()`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) opperation and passed recursively to the next itereration.  In this way, $P_2$ is never explicitly calculated, but is built up as a set of $P_1$ matrices.  The computation completes when the Schur Complement is a single block.

Instead of building the solution incrementally, and using more frequent but smaller block matrix operations, we construct large [`BlockMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) structures, and carry out the multiplication at the end of the calculation.  This should leverage the optimizations present in the [`BlockMatrix.multiply()`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.BlockMatrix) routine more effectively.  The matrices formed to carry out the operations are described int the in the figure below.

<p style="text-align: center;">
  <img src="img/lu-factorization.png"
       title="LU Algorithm Description"
       alt="LU"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>


The Matrix multiply operations shown in the figure are generalizations of equations $\eqref{eq:A11Solve}$, $\eqref{eq:U12Solve}$, and $\eqref{eq:L21Solve}$.


