package edu.uci.eecs.spectralLDA.utils

import breeze.linalg.eigSym.EigSym
import breeze.linalg.qr.QR
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector, argtopk, cholesky, eigSym, inv, qr, svd}
import breeze.numerics.{pow, sqrt}
import breeze.stats.distributions.{Gaussian, Rand, RandBasis}
import org.apache.spark.rdd.RDD


object RandNLA {
  /** Randomised Power Iteration Method for SVD of shifted M2
    *
    * As the shifted M2 is of size V-by-V, where V is the vocabulary size, which could be
    * very large, we carry out Randomised Power Iteration Method for computing the SVD of it
    * following Musco & Musco 2016. The Nystrom decomposition is also implemented but it
    * gives much worse empirical performance than Musco & Musco 2016.
    *
    * Ref:
    * Halko, N, P.G. Martinsson, & J.A. Tropp, Finding Structure with Randomness:
    * Probabilistic Algorithms for Constructing Approximate Matrix Decompositions, 2011
    * Gu, Ming, Subspace Iteration Randomization and Singular Value Problems, 2014
    * Musco, Cameron, & Christopher Musco, Randomized Block Krylov Methods for Stronger
    * and Faster Approximate Singular Value Decomposition, 2016
    *
    * @param alpha0     sum of alpha, the Dirichlet prior vector
    * @param vocabSize  V: the vocabulary size
    * @param dimK       K: number of topics
    * @param numDocs    number of documents
    * @param firstOrderMoments   average of the word count vectors
    * @param documents  RDD of the documents
    * @param nIter      number of iterations for the Randomised Power Iteration method,
    *                   denoted by q in the Algorithm 1 & 2 in the ref paper
    * @param randBasis  the random seed
    * @return           V-by-K eigenvector matrix and length-K eigenvalue vector
    */
  def whiten2(alpha0: Double,
              vocabSize: Int,
              dimK: Int,
              numDocs: Long,
              firstOrderMoments: DenseVector[Double],
              documents: RDD[(Long, Double, SparseVector[Double])],
              termsLowIDF: Seq[Int] = Seq[Int](),
              nIter: Int = 1)
            (implicit randBasis: RandBasis = Rand)
  : (DenseMatrix[Double], DenseVector[Double]) = {
    assert(vocabSize >= dimK)
    assert(nIter >= 0)

    val slackDimK = Math.min(vocabSize - dimK, dimK)

    var q = DenseMatrix.rand[Double](vocabSize, dimK + slackDimK, Gaussian(mu = 0.0, sigma = 1.0))
    var m2q: DenseMatrix[Double] = null

    for (i <- 0 until 2 * nIter + 1) {
      m2q = randomProjectM2(
        alpha0,
        vocabSize,
        dimK,
        slackDimK,
        numDocs,
        firstOrderMoments,
        documents,
        q,
        termsLowIDF
      )
      val QR(nextq, _) = qr.reduced(m2q)
      q = nextq
    }

    m2q = randomProjectM2(
      alpha0,
      vocabSize,
      dimK,
      slackDimK,
      numDocs,
      firstOrderMoments,
      documents,
      q,
      termsLowIDF
    )

    // Randomised eigendecomposition of M2
    val (s: DenseVector[Double], u: DenseMatrix[Double]) = decomp2(m2q, q)
    val idx = argtopk(s, dimK)
    val u_M2 = u(::, idx).toDenseMatrix
    val s_M2 = s(idx).toDenseVector

    (u_M2, s_M2)
  }

  /** Musco-Musco method for randomised eigendecomposition of Hermitian matrix
    *
    * We could first do the eigendecomposition (AQ)^* AQ=USU^*. If A=HKH^*; apparently
    * K=S^{1/2}, H=QU.
    *
    * Empirically for the Spectral LDA model, this decomposition algorithm often
    * gives better final results than the Nystrom method.
    *
    * Ref:
    * Musco, Cameron, & Christopher Musco, Randomized Block Krylov Methods for Stronger
    * and Faster Approximate Singular Value Decomposition, 2016
    *
    * @param aq product of the original n-by-n matrix A and a n-by-k test matrix
    * @param q  the n-by-k test matrix
    * @return   the top k eigenvalues, top k eigenvectors of the original matrix A
    */
  def decomp2(aq: DenseMatrix[Double],
              q: DenseMatrix[Double])
      : (DenseVector[Double], DenseMatrix[Double]) = {
    val w = aq.t * aq
    val EigSym(s: DenseVector[Double], u: DenseMatrix[Double]) = eigSym((w + w.t) / 2.0)

    val sqrt_s = sqrt(s)

    val h = q * u

    (sqrt_s, h)
  }

  /** Given a test matrix q returns the product of shifted M2 and q */
  private[utils] def randomProjectM2(alpha0: Double,
                                     vocabSize: Int,
                                     dimK: Int,
                                     slackDimK: Int,
                                     numDocs: Long,
                                     firstOrderMoments: DenseVector[Double],
                                     documents: RDD[(Long, Double, SparseVector[Double])],
                                     q: DenseMatrix[Double],
                                     termsLowIDF: Seq[Int] = Seq[Int]()
                                    ): DenseMatrix[Double] = {
    val para_main: Double = (alpha0 + 1.0) * alpha0
    val para_shift: Double = alpha0 * alpha0

    firstOrderMoments(termsLowIDF) := 0.0
    q(termsLowIDF, ::) := 0.0

    val qBroadcast = documents.sparkContext.broadcast[DenseMatrix[Double]](q)

    val unshiftedM2 = DenseMatrix.zeros[Double](vocabSize, dimK + slackDimK)
    documents
      .flatMap {
        doc => accumulate_M_mul_S(
          qBroadcast.value, doc._3, doc._2
        )
      }
      .reduceByKey(_ + _)
      .collect
      .foreach {
        case (token, v) => unshiftedM2(token, ::) := v.t
      }
    unshiftedM2 /= numDocs.toDouble
    unshiftedM2(termsLowIDF, ::) := 0.0

    qBroadcast.unpersist

    val m2 = unshiftedM2 * para_main - (firstOrderMoments * (firstOrderMoments.t * q)) * para_shift

    m2
  }


  /** Return the contribution of a document to M2, multiplied by the test matrix
    *
    * @param S   n-by-k test matrix
    * @param Wc  length-n word count vector
    * @param len total word count
    * @return    M2*S, i.e (Wc*Wc.t-diag(Wc))/(len*(len-1.0))*S
    */
  private[utils] def accumulate_M_mul_S(S: breeze.linalg.DenseMatrix[Double],
                                        Wc: breeze.linalg.SparseVector[Double],
                                        len: Double)
        : Seq[(Int, DenseVector[Double])] = {
    val len_calibrated: Double = math.max(len, 3.0)
    val norm_length: Double = 1.0 / (len_calibrated * (len_calibrated - 1.0))

    val data_mul_S: DenseVector[Double] = S.t * Wc

    Wc.activeIterator.toSeq.map { case (token, count) =>
      (token, (data_mul_S - S(token, ::).t) * count * norm_length)
    }
  }
}
