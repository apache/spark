/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.tuning.bandit

import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.DataFrame

abstract class Search {
  def search(
      totalBudgets: Int,
      arms: Array[Arm],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm
}

/**
 * A naive search strategy that pulling arms in a round robin style. The static search is exactly
 * the same with Cross Validation, so we can compare other methods with this one.
 */
class StaticSearch extends Search {

  override def search(
      totalBudgets: Int,
      arms: Array[Arm],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm = {

    assert(arms.length != 0, "ERROR: No arms!")
    val numArms = arms.length
    var i = 0
    while (i  < totalBudgets) {
      arms(i % numArms).pull(trainingData, i, Some(validationData), record = needRecord)
      i += 1
    }

    val bestArm = if (isLargerBetter) {
      arms.maxBy(arm => arm.getValidationResult(validationData))
    } else {
      arms.minBy(arm => arm.getValidationResult(validationData))
    }
    bestArm
  }
}



/**
 * Exponential weight search first gives us a uniform distribution. Then it samples from the
 * distribution, and accumulates the estimated loss for the selected arm. As a consequence, the arm
 * with less accumulated estimated loss will have more chance to be selected to run in the next
 * iteration. However, if an arm is much more lucky than others, i.e. be selected more times, then
 * it tends to reduce the weight to be selected in the next iteration, for the accumulating behavior
 * in the strategy. So it gives us the chance to try other arms.
 * Please refer to the *Regret Analysis of Stochastic and Nonstochastic Multi-armed Bandit Problems*
 * Chapter 3, written by Sebastien Bubeck and Nicolo Cesa-Bianchi.
 */
class ExponentialWeightsSearch extends Search {
  override def search(
      totalBudgets: Int,
      arms: Array[Arm],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length
    val eta = math.sqrt(2 * math.log(numArms) / (numArms * totalBudgets))

    val lt = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    val wt = Vectors.dense(Array.fill(numArms)(1.0)).asInstanceOf[DenseVector]
    for (t <- 0 until totalBudgets) {
      val pt = Vectors.zeros(numArms)
      axpy(Utils.sum(wt), wt, pt)
      val it = if (t < numArms) t else Utils.chooseOne(pt)
      val arm = arms(it)
      arm.pull(trainingData, t, Some(validationData), record = needRecord)
      lt.values(it) += arm.getValidationResult(validationData)
      // We use `1.0 / lt(it)` instead of `lt(it)` in the original paper, for the reason that the
      // paper uses loss, which the `isLargerBetter` is false. Here our `isLargeBetter` is true.
      // Note: Be careful with potential NaN here.
      wt.values(it) = math.exp(- eta * 1.0 / lt(it))
    }
    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}
