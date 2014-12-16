package org.apache.spark.ml.impl.estimator

import org.apache.spark.ml.Model

/**
 * Iterative stateful solver for an [[IterativeEstimator]].
 * This type of estimator holds state (a fixed dataset and a model) and allows for iterative
 * optimization.
 *
 * This type is distinct from an Optimizer in that an Optimizer has no concept of
 * a [[Model]]; an [[IterativeSolver]] can produce a [[Model]] at any time.
 *
 * This type is not an [[org.apache.spark.ml.Estimator]], but it can produce a model.
 *
 * Notes to developers:
 *  - This class allows an algorithm such as LinearRegression to produce an IterativeSolver
 *    even when the underlying optimization is non-iterative (such as matrix inversion).
 *    In that case, the step() method can be called once, after which it will do nothing.
 */
abstract class IterativeSolver[M <: Model[M]] {

  protected var currentIteration: Int = 0

  /**
   * Run one step (iteration) of learning.
   * @return  True if the step completed.
   *          False if learning had already finished, or if the step failed.
   */
  def step(): Boolean = {
    // This handles the iteration limit.  Developers can implement stepImpl().
    if (currentIteration >= maxIterations) return false
    val res = stepImpl()
    if (res) currentIteration += 1
    res
  }

  /**
   * Same as step, except that it ignores the iteration limit.
   */
  protected def stepImpl(): Boolean

  /**
   * Get the current model.
   */
  def currentModel: M

  def maxIterations: Int

  /**
   * Number of iterations completed so far.
   * If [[step()]] returns false, this count will not be incremented.
   */
  def numIterations: Int = currentIteration
}
