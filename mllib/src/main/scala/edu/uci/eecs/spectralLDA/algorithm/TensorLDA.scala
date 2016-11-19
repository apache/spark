package edu.uci.eecs.spectralLDA.algorithm

/**
 * Tensor Decomposition Algorithms.
 * Alternating Least Square algorithm is implemented.
 * Created by Furong Huang on 11/2/15.
 */
import edu.uci.eecs.spectralLDA.datamoments.DataCumulant
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector, argtopk, diag, max, min}
import breeze.numerics._
import breeze.stats.distributions.{Rand, RandBasis}
import edu.uci.eecs.spectralLDA.utils.NonNegativeAdjustment
import org.apache.spark.rdd.RDD


/** Spectral LDA model
  *
  * @param dimK                 number of topics k
  * @param alpha0               sum of alpha for the Dirichlet prior for topic distribution
  * @param maxIterations        max number of iterations for the ALS algorithm for CP decomposition
  * @param tol                  tolerance. the dot product threshold in ALS is 1-tol
  * @param idfLowerBound        lower bound of Inverse Document Frequency (IDF) for the words to be taken into account
  * @param m2ConditionNumberUB  upper bound of Condition Number for the shifted M2 matrix, if the empirical
  *                             Condition Number exceeds the uppper bound the code quits with error before computing
  *                             the M3. It allows to quickly check if there're any predominant topics
  * @param randomisedSVD        uses randomised SVD on M2, true by default
  */
class TensorLDA(dimK: Int,
                alpha0: Double,
                maxIterations: Int = 500,
                tol: Double = 1e-6,
                idfLowerBound: Double = 1.0,
                m2ConditionNumberUB: Double = Double.PositiveInfinity,
                randomisedSVD: Boolean = true,
                numIterationsKrylovMethod: Int = 1) extends Serializable {
  assert(dimK > 0, "The number of topics dimK must be positive.")
  assert(alpha0 > 0, "The topic concentration alpha0 must be positive.")
  assert(maxIterations > 0, "The number of iterations for ALS must be positive.")
  assert(tol > 0.0, "tol must be positive and probably close to 0.")

  def fit(documents: RDD[(Long, SparseVector[Double])])
         (implicit randBasis: RandBasis = Rand)
          : (DenseMatrix[Double], DenseVector[Double],
             DenseMatrix[Double], DenseVector[Double],
             DenseVector[Double]) = {
    val cumulant: DataCumulant = DataCumulant.getDataCumulant(
      dimK,
      alpha0,
      documents,
      idfLowerBound = idfLowerBound,
      m2ConditionNumberUB = m2ConditionNumberUB,
      randomisedSVD = randomisedSVD,
      numIterationsKrylovMethod = numIterationsKrylovMethod
    )

    val myALS: ALS = new ALS(
      dimK,
      cumulant.thirdOrderMoments,
      maxIterations = maxIterations,
      tol = tol
    )

    val (nu: DenseMatrix[Double], _, _, lambda: DenseVector[Double]) = myALS.run

    // unwhiten the results
    // unwhitening matrix: $(W^T)^{-1}=U\Sigma^{1/2}$
    val unwhiteningMatrix = cumulant.eigenVectorsM2 * diag(sqrt(cumulant.eigenValuesM2))

    val alphaUnordered: DenseVector[Double] = lambda.map(x => scala.math.pow(x, -2))
    val topicWordMatrixUnordered: DenseMatrix[Double] = unwhiteningMatrix * nu * diag(lambda)

    // re-arrange alpha and topicWordMatrix in descending order of alpha
    val idx = argtopk(alphaUnordered, dimK)
    val alpha = alphaUnordered(idx).toDenseVector
    val topicWordMatrix = topicWordMatrixUnordered(::, idx).toDenseMatrix

    // Diagnostic information: the ratio of the maximum to the minimum of the
    // top k eigenvalues of shifted M2
    //
    // If it's too large (>10), the algorithm may not be able to output reasonable results.
    // It could be due to some very frequent (low IDF) words or that we specified dimK
    // larger than the rank of the shifted M2.
    val m2ConditionNumber = max(cumulant.eigenValuesM2) / min(cumulant.eigenValuesM2)
    println(s"Shifted M2 top $dimK eigenvalues: ${cumulant.eigenValuesM2}")
    println(s"Shifted M2 condition number: $m2ConditionNumber")
    println("If the condition number is too large (e.g. >10), the algorithm may not be able to " +
      "output reasonable results. It could be due to the existence of very frequent words " +
      "across the documents or that the specified k is larger than the true number of topics.")

    (NonNegativeAdjustment.simplexProj_Matrix(topicWordMatrix), alpha,
      cumulant.eigenVectorsM2, cumulant.eigenValuesM2, cumulant.firstOrderMoments)
  }

}
