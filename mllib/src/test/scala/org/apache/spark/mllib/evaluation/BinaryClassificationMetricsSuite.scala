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

package org.apache.spark.mllib.evaluation

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils.DoubleWithAlmostEquals

class BinaryClassificationMetricsSuite extends FunSuite with LocalSparkContext {

  // TODO: move utility functions to TestingUtils.

  def elementsAlmostEqual(actual: Seq[Double], expected: Seq[Double]): Boolean = {
    actual.zip(expected).forall { case (x1, x2) =>
      x1.almostEquals(x2)
    }
  }

  def elementsAlmostEqual(
      actual: Seq[(Double, Double)],
      expected: Seq[(Double, Double)])(implicit dummy: DummyImplicit): Boolean = {
    actual.zip(expected).forall { case ((x1, y1), (x2, y2)) =>
      x1.almostEquals(x2) && y1.almostEquals(y2)
    }
  }

  test("binary evaluation metrics") {
    val scoreAndLabels = sc.parallelize(
      Seq((0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)), 2)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val threshold = Seq(0.8, 0.6, 0.4, 0.1)
    val numTruePositives = Seq(1, 3, 3, 4)
    val numFalsePositives = Seq(0, 1, 2, 3)
    val numPositives = 4
    val numNegatives = 3
    val precision = numTruePositives.zip(numFalsePositives).map { case (t, f) =>
      t.toDouble / (t + f)
    }
    val recall = numTruePositives.map(t => t.toDouble / numPositives)
    val fpr = numFalsePositives.map(f => f.toDouble / numNegatives)
    val rocCurve = Seq((0.0, 0.0)) ++ fpr.zip(recall) ++ Seq((1.0, 1.0))
    val pr = recall.zip(precision)
    val prCurve = Seq((0.0, 1.0)) ++ pr
    val f1 = pr.map { case (r, p) => 2.0 * (p * r) / (p + r) }
    val f2 = pr.map { case (r, p) => 5.0 * (p * r) / (4.0 * p + r)}
    assert(elementsAlmostEqual(metrics.thresholds().collect(), threshold))
    assert(elementsAlmostEqual(metrics.roc().collect(), rocCurve))
    assert(metrics.areaUnderROC().almostEquals(AreaUnderCurve.of(rocCurve)))
    assert(elementsAlmostEqual(metrics.pr().collect(), prCurve))
    assert(metrics.areaUnderPR().almostEquals(AreaUnderCurve.of(prCurve)))
    assert(elementsAlmostEqual(metrics.fMeasureByThreshold().collect(), threshold.zip(f1)))
    assert(elementsAlmostEqual(metrics.fMeasureByThreshold(2.0).collect(), threshold.zip(f2)))
    assert(elementsAlmostEqual(metrics.precisionByThreshold().collect(), threshold.zip(precision)))
    assert(elementsAlmostEqual(metrics.recallByThreshold().collect(), threshold.zip(recall)))
  }
}
