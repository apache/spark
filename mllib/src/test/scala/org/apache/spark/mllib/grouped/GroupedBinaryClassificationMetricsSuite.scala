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

package org.apache.spark.mllib.grouped

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.evaluation.AreaUnderCurve

class GroupedBinaryClassificationMetricsSuite extends FunSuite with LocalSparkContext {

  def cond1(x: (Double, Double)): Boolean = x._1 ~= (x._2) absTol 1E-5

  def cond2(x: ((Double, Double), (Double, Double))): Boolean =
    (x._1._1 ~= x._2._1 absTol 1E-5) && (x._1._2 ~= x._2._2 absTol 1E-5)

  /*
   This test was copied from BinaryClassificationMetricsSuite, but adds in additional data sets to the RDD
   */
  test("grouped binary evaluation metrics") {
    val scoreAndLabels = sc.parallelize(
      Seq(
        (1,(0.1, 0.0)), (1,(0.1, 1.0)), (1,(0.4, 0.0)), (1,(0.6, 0.0)), (1,(0.6, 1.0)), (1,(0.6, 1.0)), (1,(0.8, 1.0)),
        (2,(0.2, 0.0)), (2,(0.1, 1.0)), (2,(0.3, 0.0)), (2,(0.7, 1.0)), (2,(0.5, 1.0)), (2,(0.3, 1.0)), (2,(0.9, 1.0)), (2,(0.1, 0.0)),
        (3,(0.3, 0.0)), (3,(0.3, 1.0)), (3,(0.5, 1.0)), (3,(0.3, 0.0)), (3,(0.1, 0.0)), (3,(0.9, 1.0)), (3,(0.4, 0.0))
      ), 2)
    val metrics = new GroupedBinaryClassificationMetrics(scoreAndLabels)
    val threshold_1 = Seq(0.8, 0.6, 0.4, 0.1)
    val threshold_2 = Seq(0.9, 0.7, 0.5, 0.3, 0.2, 0.1)
    val numTruePositives_1 = Seq(1, 3, 3, 4)
    val numFalsePositives_1 = Seq(0, 1, 2, 3)
    val numPositives_1 = 4
    val numNegatives_1 = 3
    val precision_1 = numTruePositives_1.zip(numFalsePositives_1).map { case (t, f) =>
      t.toDouble / (t + f)
    }

    val recall_1 = numTruePositives_1.map(t => t.toDouble / numPositives_1)
    val fpr_1 = numFalsePositives_1.map(f => f.toDouble / numNegatives_1)
    val rocCurve_1 = Seq((0.0, 0.0)) ++ fpr_1.zip(recall_1) ++ Seq((1.0, 1.0))
    val pr_1 = recall_1.zip(precision_1)
    val prCurve_1 = Seq((0.0, 1.0)) ++ pr_1
    val f1_1 = pr_1.map { case (r, p) => 2.0 * (p * r) / (p + r)}
    val f2_1 = pr_1.map { case (r, p) => 5.0 * (p * r) / (4.0 * p + r)}

    assert(metrics.thresholds().collect().filter(_._1 == 1).map(_._2).zip(threshold_1).forall(cond1))
    assert(metrics.thresholds().collect().filter(_._1 == 2).map(_._2).zip(threshold_2).forall(cond1))
    assert(metrics.roc().filter(_._1 == 1).map(_._2).collect().zip(rocCurve_1).forall(cond2))
    assert(metrics.areaUnderROC()(1) ~== AreaUnderCurve.of(rocCurve_1) absTol 1E-5)
    assert(metrics.pr().filter(_._1 == 1).map(_._2).collect().zip(prCurve_1).forall(cond2))
    assert(metrics.areaUnderPR()(1) ~== AreaUnderCurve.of(prCurve_1) absTol 1E-5)
    assert(metrics.fMeasureByThreshold().filter(_._1 == 1).map(_._2).collect().zip(threshold_1.zip(f1_1)).forall(cond2))
    assert(metrics.fMeasureByThreshold(2.0).filter(_._1 == 1).map(_._2).collect().zip(threshold_1.zip(f2_1)).forall(cond2))
    assert(metrics.precisionByThreshold().filter(_._1 == 1).map(_._2).collect().zip(threshold_1.zip(precision_1)).forall(cond2))
    assert(metrics.recallByThreshold().filter(_._1 == 1).map(_._2).collect().zip(threshold_1.zip(recall_1)).forall(cond2))
  }
}
