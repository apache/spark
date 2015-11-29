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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.Model
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.DataFrame

abstract class Search {
  def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M]
}

/**
 * A naive search strategy that pulling arms in a round robin style.
 */
class StaticSearch extends Search {

  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

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
 * Simple search strategy that ...
 */
class SimpleBanditSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length
    val alpha = 0.3
    val initialRounds = math.max(1, (alpha * totalBudgets / numArms).toInt)

    for (i <- 0 until initialRounds) {
      arms.foreach(_.pull(trainingData, i, Some(validationData), record = needRecord))
    }

    var currentBudget = initialRounds * numArms
    val numPreSelectedArms = math.max(1, (alpha * numArms).toInt)

    val preSelectedArms = arms.sortBy(_.getValidationResult(validationData))
      .reverse.dropRight(numArms - numPreSelectedArms)

    while (currentBudget < totalBudgets) {
      preSelectedArms(currentBudget % numPreSelectedArms)
        .pull(trainingData, currentBudget, Some(validationData), record = needRecord)
      currentBudget += 1
    }

    val bestArm = preSelectedArms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

/**
 * Exponential weight search strategy that ...
 */
class ExponentialWeightsSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

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
      wt.values(it) = math.exp(- eta * lt(it))
    }
    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

/**
 *
 */
class LILUCBSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length

    val nj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    val sumj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    for (i <- 0 until numArms) {
      arms(i).pull(trainingData, i, Some(validationData), record = needRecord)
      sumj.values(i) += arms(i).getValidationResult(validationData)
      nj.values(i) += 1
    }

    val delta = 0.1
    var t = numArms
    val ct = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    copy(nj, ct)
    scal(3.0, ct)
    Utils.log(ct)
    scal(5.0 / delta, ct)
    Utils.log(ct)
    scal(0.5, ct)
    Utils.div(nj, ct)
    Utils.sqrt(ct)
    scal(1.5, ct)

    val ucbj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    copy(sumj, ucbj)
    Utils.div(nj, ucbj)
    Utils.sub(ct, ucbj)

    while (t < totalBudgets) {
      val it = Utils.argMin(ucbj)
      arms(it).pull(trainingData, t, Some(validationData), record = needRecord)
      sumj.values(it) += arms(it).getValidationResult(validationData)
      nj.values(it) += 1
      ct.values(it) = 1.5 * math.sqrt(0.5 * math.log(5.0 * math.log(3.0 * nj(it)) / delta) / nj(it))
      ucbj.values(it) = sumj(it) / nj(it) - ct(it)
      t += 1
    }

    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

/**
 *
 */
class LUCBSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length

    val nj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    val sumj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    for (i <- 0 until numArms) {
      arms(i).pull(trainingData, i, Some(validationData), record = needRecord)
      sumj.values(i) += arms(i).getValidationResult(validationData)
      nj.values(i) += 1
    }

    val delta = 0.1
    var t = numArms
    val ct = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    copy(nj, ct)
    scal(3.0, ct)
    Utils.log(ct)
    scal(1.0 / delta, ct)
    Utils.log(ct)
    scal(0.5, ct)
    Utils.div(nj, ct)
    Utils.sqrt(ct)
    scal(1.5, ct)

    val ucbj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    copy(sumj, ucbj)
    Utils.div(nj, ucbj)
    Utils.sub(ct, ucbj)

    while (t + 2 <= totalBudgets) {
      val sumjDivNj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
      copy(sumj, sumjDivNj)
      Utils.div(nj, sumjDivNj)

      val inds0 = Utils.argSort(sumjDivNj)
      val inds1 = Utils.argSort(ucbj)

      var it = inds0(0)
      arms(it).pull(trainingData, t, Some(validationData), record = needRecord)
      sumj.values(it) += arms(it).getValidationResult(validationData)
      nj.values(it) += 1
      t += 1

      val k1 = 1.25
      val t2nd = math.max(t * t / 4.0, 1.0)
      val t4th = t2nd * t2nd
      var ctTmp = math.sqrt(0.5 * math.log(k1 * numArms * t4th / delta) / arms(it).getNumPulls)
      ucbj.values(it) = sumj(it) / nj(it) - ctTmp

      it = if (inds1(0) == inds0(0)) inds1(1) else inds1(0)
      arms(it).pull(trainingData, t, Some(validationData), record = needRecord)
      sumj.values(it) += arms(it).getValidationResult(validationData)
      nj.values(it) += 1
      t += 1
      ctTmp = math.sqrt(0.5 * math.log(k1 * numArms * t4th / delta) / arms(it).getNumPulls)
      ucbj.values(it) = sumj(it) / nj(it) - ctTmp
    }

    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

/**
 *
 */
class SuccessiveHalvingSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length
    val numOfHalvingIter = math.ceil(Utils.log2(numArms)).toInt

    var armsRef = arms

    var t = 0

    if ((totalBudgets / (armsRef.length * numOfHalvingIter)) > 0) {
      for (_ <- 0 until numOfHalvingIter) {
        val numOfCurrentPulling = totalBudgets / (armsRef.length * numOfHalvingIter)
        var i = 0
        while (i < armsRef.length) {
          for (_ <- 0 until numOfCurrentPulling) {
            armsRef(i).pull(trainingData, t, Some(validationData), record = needRecord)
            t += 1
          }
          i += 1
        }
        armsRef = armsRef.sortBy(_.getValidationResult(validationData))
          .drop(armsRef.length - math.ceil(armsRef.length / 2.0).toInt)
      }
    }

    val bestArm = if (t == totalBudgets) {
      armsRef.maxBy(arm => arm.getValidationResult(validationData))
    } else {
      while (t < totalBudgets) {
        armsRef(t % armsRef.length).pull(trainingData, t, Some(validationData), record = needRecord)
        t += 1
      }
      armsRef.maxBy(arm => arm.getValidationResult(validationData))
    }

    bestArm
  }
}

/**
 *
 */
class SuccessiveRejectSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length

    var armsRef = arms

    val barLogOfNumArms = 0.5 + (2 to numArms).map(i => 1.0 / i).sum

    var prevNk = 0
    var t = 0
    for (k <- 1 until numArms) {
      val currNk = math.ceil((totalBudgets - numArms) / ((numArms + 1 - k) * barLogOfNumArms)).toInt
      val numOfCurrentPulling = currNk - prevNk
      var i = 0
      while (i < armsRef.length) {
        for (_ <- 0 until numOfCurrentPulling) {
          armsRef(i).pull(trainingData, t, Some(validationData), record = needRecord)
          t += 1
        }
        i += 1
      }
      armsRef = armsRef.sortBy(_.getValidationResult(validationData)).drop(1)
      prevNk = currNk
    }

    val bestArm = if (t == totalBudgets) {
      armsRef.maxBy(arm => arm.getValidationResult(validationData))
    } else {
      while (t < totalBudgets) {
        armsRef(t % armsRef.length).pull(trainingData, t, Some(validationData), record = needRecord)
        t += 1
      }
      armsRef.maxBy(arm => arm.getValidationResult(validationData))
    }

    bestArm
  }
}

/**
 *
 */
class SuccessiveEliminationSearch extends Search {
  override def search[M <: Model[M]](
      totalBudgets: Int,
      arms: Array[Arm[M]],
      trainingData: DataFrame,
      validationData: DataFrame,
      isLargerBetter: Boolean = true,
      needRecord: Boolean = false): Arm[M] = {

    if (!isLargerBetter) {
      throw new UnsupportedOperationException("Unsupported OP fow now.")
    }

    val numArms = arms.length
    val delta = 0.1

    var armsRef = arms

    armsRef.zipWithIndex.foreach { case (arm, idx) =>
      arm.pull(trainingData, idx, Some(validationData), record = needRecord)
    }
    var t = numArms

    val maxArmValidationResult = armsRef.map(_.getValidationResult(validationData)).max
    val ct = math.sqrt(0.5
      * math.log(4.0 * numArms * armsRef(0).getNumPulls * armsRef(0).getNumPulls / delta)
      / armsRef(0).getNumPulls)
    val armValuesBuilder = new ArrayBuffer[Arm[M]]()
    var i = 0
    while (i < armsRef.length) {
      if (maxArmValidationResult - armsRef(i).getValidationResult(validationData) < ct) {
        armValuesBuilder += armsRef(i)
      }
      i += 1
    }
    armsRef = armValuesBuilder.toArray

    while (2 * t <= totalBudgets) {
      val numIter = t
      for (i <- 0 until numIter) {
        armsRef(i % armsRef.length).pull(trainingData, t, Some(validationData), record = needRecord)
        t += 1
      }

      val maxArmValidationResult = armsRef.map(_.getValidationResult(validationData)).max
      val ct = math.sqrt(0.5
        * math.log(4.0 * numArms * armsRef(0).getNumPulls * armsRef(0).getNumPulls / delta)
        / armsRef(0).getNumPulls)
      val armValuesBuilder = new ArrayBuffer[Arm[M]]()
      var i = 0
      while (i < armsRef.length) {
        if (maxArmValidationResult - armsRef(i).getValidationResult(validationData) < ct) {
          armValuesBuilder += armsRef(i)
        }
        i += 1
      }
      armsRef = armValuesBuilder.toArray
    }

    val bestArm = if (t == totalBudgets) {
      armsRef.maxBy(arm => arm.getValidationResult(validationData))
    } else {
      while (t < totalBudgets) {
        armsRef(t % armsRef.length).pull(trainingData, t, Some(validationData), record = needRecord)
        t += 1
      }
      armsRef.maxBy(arm => arm.getValidationResult(validationData))
    }

    bestArm
  }
}

