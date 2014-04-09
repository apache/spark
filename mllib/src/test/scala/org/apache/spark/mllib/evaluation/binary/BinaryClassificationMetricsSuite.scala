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

package org.apache.spark.mllib.evaluation.binary

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.evaluation.AreaUnderCurve

class BinaryClassificationMetricsSuite extends FunSuite with LocalSparkContext {
  test("binary evaluation metrics") {
    val scoreAndLabels = sc.parallelize(
      Seq((0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)), 2)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val score = Seq(0.8, 0.6, 0.4, 0.1)
    val tp = Seq(1, 3, 3, 4)
    val fp = Seq(0, 1, 2, 3)
    val p = 4
    val n = 3
    val precision = tp.zip(fp).map { case (t, f) => t.toDouble / (t + f) }
    val recall = tp.map(t => t.toDouble / p)
    val fpr = fp.map(f => f.toDouble / n)
    val rocCurve = Seq((0.0, 0.0)) ++ fpr.zip(recall) ++ Seq((1.0, 1.0))
    val pr = recall.zip(precision)
    val prCurve = Seq((0.0, 1.0)) ++ pr
    val f1 = pr.map { case (re, prec) => 2.0 * (prec * re) / (prec + re) }
    val f2 = pr.map { case (re, prec) => 5.0 * (prec * re) / (4.0 * prec + re)}
    assert(metrics.thresholds().collect().toSeq === score)
    assert(metrics.roc().collect().toSeq === rocCurve)
    assert(metrics.areaUnderROC() === AreaUnderCurve.of(rocCurve))
    assert(metrics.pr().collect().toSeq === prCurve)
    assert(metrics.areaUnderPR() === AreaUnderCurve.of(prCurve))
    assert(metrics.fMeasureByThreshold().collect().toSeq === score.zip(f1))
    assert(metrics.fMeasureByThreshold(2.0).collect().toSeq === score.zip(f2))
    assert(metrics.precisionByThreshold().collect().toSeq === score.zip(precision))
    assert(metrics.recallByThreshold().collect().toSeq === score.zip(recall))
  }
}
