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

package org.apache.spark.mllib.pmml.export

// Scala Array is conflict with Array imported in PMML.
import scala.{Array => SArray}

import org.dmg.pmml._

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel => SNaiveBayesModel}
import org.apache.spark.mllib.util.TestingUtils._

class NaiveBayesPMMLModelExportSuite extends SparkFunSuite {

  test("Naive Bayes PMML export: Bernoulli model type") {
    val label = SArray(0.0, 1.0, 2.0)
    val pi = SArray(0.5, 0.1, 0.4).map(math.log)
    val theta = SArray(
      SArray(0.70, 0.10, 0.10, 0.10), // label 0
      SArray(0.10, 0.70, 0.10, 0.10), // label 1
      SArray(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val nbModel = new SNaiveBayesModel(label, pi, theta, NaiveBayes.Bernoulli)
    val nbModelExport = PMMLModelExportFactory.createPMMLModelExport(nbModel)
    val pmml = nbModelExport.getPmml

    assert(pmml.getHeader.getDescription === "naive bayes")
    assert(pmml.getDataDictionary.getNumberOfFields === theta(0).length + 1)

    // assert Bayes input
    val pmmlRegressionModel = pmml.getModels.get(0).asInstanceOf[NaiveBayesModel]
    val bayesInputs = pmmlRegressionModel.getBayesInputs
    assert(bayesInputs.getBayesInputs.size() === 4)

    val bIter = bayesInputs.iterator()
    var i = 0
    while (bIter.hasNext) {
      val bayesInput = bIter.next()
      assert(bayesInput.getFieldName.getValue === "field_" + i)
      val pairCounts = bayesInput.getPairCounts
      assert(pairCounts.size() === 2,
        "Only two values in one variables is allowed in Bernoulli model type.")
      var k = 0
      val pIter = bayesInput.getPairCounts.iterator()
      while (pIter.hasNext) {
        val pairs = pIter.next()
        val tIter = pairs.getTargetValueCounts.iterator()
        var j = 0
        while (tIter.hasNext) {
          val targetValueCount = tIter.next()
          if (k == 0) {
            // test values of pairsExist
            assert(math.log(targetValueCount.getCount) ~== theta(j)(i) relTol 1e-5)
          } else {
            // test values of pairsAbsent
            assert(math.log(1 - targetValueCount.getCount) ~== theta(j)(i) relTol 1e-5)
          }
          j += 1
        }
        k += 1
      }
      i += 1
    }

    // assert Bayes output
    val bayesOutput = pmmlRegressionModel.getBayesOutput.getTargetValueCounts
    assert(bayesOutput.getTargetValueCounts.size() === pi.length)

    val bayesOutputIter = bayesOutput.iterator()
    i = 0
    while (bayesOutputIter.hasNext) {
      val targetCount = bayesOutputIter.next()
      assert(targetCount.getValue.toDouble === i)
      assert(targetCount.getCount === math.exp(pi(i)))
      i += 1
    }
  }
}
