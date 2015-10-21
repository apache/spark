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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class Search(val trainingData: DataFrame, val validationData: DataFrame) {
  def search(totalBudgets: Int, arms: Array[Arm]): Arm
}

class StaticSearch(override val trainingData: DataFrame, override val validationData: DataFrame) extends Search(trainingData, validationData) {
  override def search(totalBudgets: Int, arms: Array[Arm]): Arm = {

    assert(arms.size != 0, "ERROR: No arms!")
    val numArms = arms.size
    var i = 0
    while (i  < totalBudgets) {
      arms(i % numArms).pull(trainingData)
      i += 1
    }

    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

class SimpleBanditSearch(override val trainingData: DataFrame, override val validationData: DataFrame) extends Search(trainingData, validationData) {
  override def search(totalBudgets: Int, arms: Array[Arm]): Arm = {
    val numArms = arms.size
    val alpha = 0.3
    val initialRounds = math.max(1, (alpha * totalBudgets / numArms).toInt)

    for (i <- 0 until initialRounds) {
      arms.foreach(_.pull(trainingData))
    }

    var currentBudget = initialRounds * numArms
    val numPreSelectedArms = math.max(1, (alpha * numArms).toInt)

    val preSelectedArms = arms.sortBy(_.getValidationResult(validationData))
      .reverse.dropRight(numArms - numPreSelectedArms)

    while (currentBudget < totalBudgets) {
      preSelectedArms(currentBudget % numPreSelectedArms).pull(trainingData)
      currentBudget += 1
    }

    val bestArm = preSelectedArms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

class ExponentialWeightsSearch(override val trainingData: DataFrame, override val validationData: DataFrame) extends Search(trainingData, validationData) {
  override def search(totalBudgets: Int, arms: Array[Arm]): Arm = {
    val numArms = arms.size
    val eta = math.sqrt(2 * math.log(numArms) / (numArms * totalBudgets))

    val lt = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    val wt = Vectors.dense(Array.fill(numArms)(1.0)).asInstanceOf[DenseVector]
    for (t <- 0 until totalBudgets) {
      val pt = Vectors.zeros(numArms)
      axpy(Utils.sum(wt), wt, pt)
      val it = if (t < numArms) t else Utils.chooseOne(pt)
      val arm = arms(it)
      arm.pull(trainingData)
      lt.values(it) += arm.getValidationResult(validationData)
      wt.values(it) = math.exp(- eta * lt(it))
    }
    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

class LILUCBSearch(override val trainingData: DataFrame, override val validationData: DataFrame) extends Search(trainingData, validationData) {
  override def search(totalBudgets: Int, arms: Array[Arm]): Arm = {
    val numArms = arms.size

    val nj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    val sumj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    for (i <- 0 until numArms) {
      arms(i).pull(trainingData)
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
      arms(it).pull(trainingData)
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

class LUCBSearch(override val trainingData: DataFrame, override val validationData: DataFrame) extends Search(trainingData, validationData) {
  override def search(totalBudgets: Int, arms: Array[Arm]): Arm = {
    val numArms = arms.size

    val nj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    val sumj = Vectors.zeros(numArms).asInstanceOf[DenseVector]
    for (i <- 0 until numArms) {
      arms(i).pull(trainingData)
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
      arms(it).pull(trainingData)
      sumj.values(it) += arms(it).getValidationResult(validationData)
      nj.values(it) += 1
      t += 1

      val k1 = 1.25
      val t2nd = math.max(t * t / 4.0, 1.0)
      val t4th = t2nd * t2nd
      var ctTmp = math.sqrt(0.5 * math.log(k1 * numArms * t4th / delta) / arms(it).numPulls)
      ucbj.values(it) = sumj(it) / nj(it) - ctTmp

      it = if (inds1(0) == inds0(0)) inds1(1) else inds1(0)
      arms(it).pull(trainingData)
      sumj.values(it) += arms(it).getValidationResult(validationData)
      nj.values(it) += 1
      t += 1
      ctTmp = math.sqrt(0.5 * math.log(k1 * numArms * t4th / delta) / arms(it).numPulls)
      ucbj.values(it) = sumj(it) / nj(it) - ctTmp
    }

    val bestArm = arms.maxBy(arm => arm.getValidationResult(validationData))
    bestArm
  }
}

/*
class SuccessiveHalvingSearch(override val trainingData: DataFrame, override val validationData: DataFrame) extends Search(trainingData, validationData) {
  override def search(totalBudgets: Int, arms: Array[Arm]): Arm = {
    val numArms = arms.size
    val numOfHalvingIter = math.ceil(Utils.log2(numArms)).toInt

    var armAry = arms

    var t = 0

    if ((totalBudgets / (arms.size * numOfHalvingIter)) > 0) {
      for (_ <- 0 until numOfHalvingIter) {
        val numOfCurrentPulling = totalBudgets / (arms.size * numOfHalvingIter)
        var i = 0
        while (i < arms.size) {
          for (_ <- 0 until numOfCurrentPulling) {
            arms(i).pull(trainingData)
            t += 1
          }
          i += 1
        }
        arms = arms.sortBy(_.getValidationResult(validationData))
          .drop(arms.size - math.ceil(arms.size / 2.0).toInt)
      }
    }

    val bestArm = if (t == totalBudgets) {
      armValues.maxBy(arm => arm.getValidationResult(recompute = false))
    } else {
      while (t < totalBudgets) {
        armValues(t % armValues.size).pull()
        t += 1
      }
      armValues.maxBy(arm => arm.getValidationResult())
    }

    bestArm
  }
}

class SuccessiveRejectSearch extends Search {
  override val name = "successive reject search"
  override def search(totalBudgets: Int, arms: Map[(String, String), Arm[_]]): Arm[_] = {
    val numArms = arms.size
    var armValues = arms.values.toArray
    val barLogOfNumArms = 0.5 + (2 to numArms).map(i => 1.0 / i).sum

    var prevNk = 0
    var t = 0
    for (k <- 1 until numArms) {
      val currNk = math.ceil((totalBudgets - numArms) / ((numArms + 1 - k) * barLogOfNumArms)).toInt
      val numOfCurrentPulling = currNk - prevNk
      var i = 0
      while (i < armValues.size) {
        for (_ <- 0 until numOfCurrentPulling) {
          armValues(i).pull()
          t += 1
        }
        i += 1
      }
      armValues = armValues.sortBy(_.getValidationResult()).drop(1)
      prevNk = currNk
    }

    val bestArm = if (t == totalBudgets) {
      armValues.maxBy(arm => arm.getValidationResult(recompute = false))
    } else {
      while (t < totalBudgets) {
        armValues(t % armValues.size).pull()
        t += 1
      }
      armValues.maxBy(arm => arm.getValidationResult())
    }

    bestArm
  }
}

class SuccessiveEliminationSearch extends Search {
  override val name = "successive elimination search"
  override def search(totalBudgets: Int, arms: Map[(String, String), Arm[_]]): Arm[_] = {
    val numArms = arms.size
    val delta = 0.1
    var armValues = arms.values.toArray

    armValues.foreach(_.pull())
    var t = numArms

    val maxArmValidationResult = armValues.map(_.getValidationResult()).max
    val ct = math.sqrt(0.5
      * math.log(4.0 * numArms * armValues(0).numPulls * armValues(0).numPulls / delta)
      / armValues(0).numPulls)
    val armValuesBuilder = new ArrayBuffer[Arm[_]]()
    var i = 0
    while (i < armValues.size) {
      if (maxArmValidationResult - armValues(i).getValidationResult(recompute = false) < ct) {
        armValuesBuilder += armValues(i)
      }
      i += 1
    }
    armValues = armValuesBuilder.toArray

    while (2 * t <= totalBudgets) {
      val numIter = t
      for (i <- 0 until numIter) {
        armValues(i % armValues.size).pull()
        t += 1
      }

      val maxArmValidationResult = armValues.map(_.getValidationResult()).max
      val ct = math.sqrt(0.5
        * math.log(4.0 * numArms * armValues(0).numPulls * armValues(0).numPulls / delta)
        / armValues(0).numPulls)
      val armValuesBuilder = new ArrayBuffer[Arm[_]]()
      var i = 0
      while (i < armValues.size) {
        if (maxArmValidationResult - armValues(i).getValidationResult(recompute = false) < ct) {
          armValuesBuilder += armValues(i)
        }
        i += 1
      }
      armValues = armValuesBuilder.toArray
    }

    val bestArm = if (t == totalBudgets) {
      armValues.maxBy(arm => arm.getValidationResult(recompute = false))
    } else {
      while (t < totalBudgets) {
        armValues(t % armValues.size).pull()
        t += 1
      }
      armValues.maxBy(arm => arm.getValidationResult())
    }

    bestArm
  }
}

*/