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

class BinaryClassificationEvaluatorSuite extends FunSuite with LocalSparkContext {
  test("binary evaluation metrics") {
    val scoreAndLabels = sc.parallelize(
      Seq((0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)), 2)
    val evaluator = new BinaryClassificationEvaluator(scoreAndLabels)
    val score = Seq(0.8, 0.6, 0.4, 0.1)
    val tp = Seq(1, 3, 3, 4)
    val fp = Seq(0, 1, 2, 3)
    val p = 4
    val n = 3
    val precision = tp.zip(fp).map { case (t, f) => t.toDouble / (t + f) }
    val recall = tp.map(t => t.toDouble / p)
    val fpr = fp.map(f => f.toDouble / n)
    val roc = fpr.zip(recall)
    val pr = recall.zip(precision)
    val f1 = pr.map { case (re, prec) => 2.0 * (prec * re) / (prec + re) }
    val f2 = pr.map { case (re, prec) => 5.0 * (prec * re) / (4.0 * prec + re)}
    assert(evaluator.rocCurve().collect().toSeq === roc)
    assert(evaluator.rocAUC() === AreaUnderCurve.of(roc))
    assert(evaluator.prCurve().collect().toSeq === pr)
    assert(evaluator.prAUC() === AreaUnderCurve.of(pr))
    assert(evaluator.fMeasureByThreshold().collect().toSeq === score.zip(f1))
    assert(evaluator.fMeasureByThreshold(2.0).collect().toSeq === score.zip(f2))
  }
}
