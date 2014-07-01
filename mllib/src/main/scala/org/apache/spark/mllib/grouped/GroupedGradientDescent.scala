package org.apache.spark.mllib.grouped

import org.apache.spark.mllib.optimization.{Updater, Gradient}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.SparkContext._

class GroupedGradientDescent(private var gradient: Gradient, private var updater: Updater)
  extends GroupedOptimizer with Logging {
  /**
   * Solve the provided convex optimization problem.
   */

    private var stepSize: Double = 1.0
    private var numIterations: Int = 100
    private var regParam: Double = 0.0
    private var miniBatchFraction: Double = 1.0


  /**
   * Set the initial step size of SGD for the first step. Default 1.0.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * :: Experimental ::
   * Set fraction of data to be used for each SGD iteration.
   * Default 1.0 (corresponding to deterministic/classical gradient descent)
   */
  @Experimental
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  /**
   * :: DeveloperApi ::
   * Runs gradient descent on the given training data.
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  @DeveloperApi
   override def optimize(data: RDD[(Int, (Double, linalg.Vector))], initialWeights: Map[Int, linalg.Vector]): Map[Int, linalg.Vector] = {
      val out = GroupedGradientDescent.runMiniBatchSGD(
        data,
        gradient,
        updater,
        stepSize,
        numIterations,
        regParam,
        miniBatchFraction,
        initialWeights)
      out.map( x => (x._1, x._2._1) )
    }
}


object GroupedGradientDescent extends Logging {

  def runMiniBatchSGD(
                       data: RDD[(Int, (Double, Vector))],
                       gradient: Gradient,
                       updater: Updater,
                       stepSize: Double,
                       numIterations: Int,
                       regParam: Double,
                       miniBatchFraction: Double,
                       initialWeightsSet: Map[Int,Vector]): Map[Int,(Vector, Array[Double])] = {

    val stochasticLossHistory = data.keys.collect().map( x => (x, new ArrayBuffer[Double](numIterations)) ).toMap

    val numExamples = data.countByKey()
    val miniBatchSize = numExamples.map( x => (x._1, x._2 * miniBatchFraction) )

    // Initialize weights as a column vector
    var weightSet = initialWeightsSet.map(x => (x._1, Vectors.dense(x._2.toArray)))

    var regVal = weightSet.map( weights => (weights._1, updater.compute(
      weights._2, Vectors.dense(new Array[Double](weights._2.size)), 0, 1, regParam)._2 ))

    val dataWithKey = data.map( x => (x._1, (x._1, x._2._1, x._2._2)))

    for (i <- 1 to numIterations) {
      val gradientOut = dataWithKey.sample(false, miniBatchFraction, 42 + i).combineByKey[(BDV[Double], Double)](
          createCombiner = (x : (Int, Double, Vector)) => {
            val key = x._1
            val label = x._2
            val features = x._3
            val (new_gradient, new_loss) = gradient.compute(features, label, weightSet(key))
            (BDV(new_gradient.toArray), new_loss)
          },
          mergeValue = (x : (BDV[Double],Double), y : (Int,Double,Vector)) => {
            var in_gradient = x._1
            val loss = x._2
            val key = y._1
            val label = y._2
            val features = Vectors.dense(y._3.toArray)
            val (new_gradient, new_loss) = gradient.compute(features, label, weightSet(key))
            (in_gradient + BDV(new_gradient.toArray), loss + new_loss)
          },
          mergeCombiners = (x:(BDV[Double],Double), y:(BDV[Double],Double)) => {
            (x._1 + y._1, x._2 + y._2)
          }
        )
      val lossSums = gradientOut.map(x => (x._1, x._2._2)).collect.toMap
      stochasticLossHistory.foreach( x => { x._2.append(lossSums(x._1) / miniBatchSize(x._1) + regVal(x._1)) } )

      val update = gradientOut.map( x => {
        val a : BDV[Double] = BDV(x._2._1.toArray) / miniBatchSize(x._1)
        val supdate = updater.compute(
          weightSet(x._1), Vectors.dense(a.toArray), stepSize, i, regParam)
        (x._1, supdate)
      })
      val update_c = update.collect()
      weightSet = update_c.map( x => (x._1, x._2._1)).toMap
      regVal = update_c.map( x => (x._1, x._2._2)).toMap
    }
    weightSet.map( x => (x._1, (x._2, stochasticLossHistory(x._1).toArray)))
  }
}