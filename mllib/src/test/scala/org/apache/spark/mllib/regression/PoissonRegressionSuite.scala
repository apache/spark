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

package org.apache.spark.mllib.regression

import org.scalatest.FunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{PoissonRegressionDataGenerator, LocalSparkContext}
import scala.util.Random

class PoissonRegressionSuite extends FunSuite with LocalSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 2 away from expected value
      // and the diff is larger than one fifth of the label value.
      val absDiff = math.abs(prediction - expected.label)
      absDiff > 3.0 && absDiff / expected.label > 0.2 
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5,
           "Totally " + input.length + " test samples, with " + numOffPredictions + " wrong predictions.")
  }

  lazy val realDataSet = {
    sc.parallelize(Array(
      "(8,[0,1,0,0,0,0,1,28.3,3.05])",
      "(4,[0,0,1,0,0,0,1,26.0,2.60])",
      "(0,[0,0,1,0,0,0,1,25.6,2.15])",
      "(0,[0,0,0,1,0,1,0,21.0,1.85])",
      "(1,[0,1,0,0,0,0,1,29.0,3.00])",
      "(3,[0,0,0,1,0,1,0,25.0,2.30])",
      "(0,[0,0,0,1,0,0,1,26.2,1.30])",
      "(0,[0,1,0,0,0,0,1,24.9,2.10])",
      "(8,[0,1,0,0,0,0,1,25.7,2.00])",
      "(6,[0,1,0,0,0,0,1,27.5,3.15])",
      "(5,[0,0,0,1,0,0,1,26.1,2.80])",
      "(4,[0,0,1,0,0,0,1,28.9,2.80])",
      "(3,[0,1,0,0,0,0,1,30.3,3.60])",
      "(4,[0,1,0,0,0,0,1,22.9,1.60])",
      "(3,[0,0,1,0,0,0,1,26.2,2.30])",
      "(5,[0,0,1,0,0,0,1,24.5,2.05])",
      "(8,[0,1,0,0,0,0,1,30.0,3.05])",
      "(3,[0,1,0,0,0,0,1,26.2,2.40])",
      "(6,[0,1,0,0,0,0,1,25.4,2.25])",
      "(4,[0,1,0,0,0,0,1,25.4,2.25])",
      "(0,[0,0,0,1,0,0,1,27.5,2.90])",
      "(3,[0,0,0,1,0,0,1,27.0,2.25])",
      "(0,[0,1,0,0,0,1,0,24.0,1.70])",
      "(0,[0,1,0,0,0,0,1,28.7,3.20])",
      "(1,[0,0,1,0,0,0,1,26.5,1.97])",
      "(1,[0,1,0,0,0,0,1,24.5,1.60])",
      "(1,[0,0,1,0,0,0,1,27.3,2.90])",
      "(4,[0,1,0,0,0,0,1,26.5,2.30])",
      "(2,[0,1,0,0,0,0,1,25.0,2.10])",
      "(0,[0,0,1,0,0,0,1,22.0,1.40])",
      "(2,[0,0,0,1,0,0,1,30.2,3.28])",
      "(0,[0,1,0,0,0,1,0,25.4,2.30])",
      "(6,[0,1,0,0,0,0,1,24.9,2.30])",
      "(10,[0,0,0,1,0,0,1,25.8,2.25])",
      "(5,[0,0,1,0,0,0,1,27.2,2.40])",
      "(3,[0,1,0,0,0,0,1,30.5,3.32])",
      "(8,[0,0,0,1,0,0,1,25.0,2.10])",
      "(9,[0,1,0,0,0,0,1,30.0,3.00])",
      "(0,[0,1,0,0,0,0,1,22.9,1.60])",
      "(2,[0,1,0,0,0,0,1,23.9,1.85])",
      "(3,[0,1,0,0,0,0,1,26.0,2.28])",
      "(0,[0,1,0,0,0,0,1,25.8,2.20])",
      "(4,[0,0,1,0,0,0,1,29.0,3.28])",
      "(0,[0,0,0,1,0,0,1,26.5,2.35])",
      "(0,[0,0,1,0,0,0,1,22.5,1.55])",
      "(0,[0,1,0,0,0,0,1,23.8,2.10])",
      "(0,[0,0,1,0,0,0,1,24.3,2.15])",
      "(14,[0,1,0,0,0,0,1,26.0,2.30])",
      "(0,[0,0,0,1,0,0,1,24.7,2.20])",
      "(1,[0,1,0,0,0,0,1,22.5,1.60])",
      "(3,[0,1,0,0,0,0,1,28.7,3.15])",
      "(4,[0,0,0,1,0,0,1,29.3,3.20])",
      "(5,[0,1,0,0,0,0,1,26.7,2.70])",
      "(0,[0,0,0,1,0,0,1,23.4,1.90])",
      "(6,[0,0,0,1,0,0,1,27.7,2.50])",
      "(6,[0,1,0,0,0,0,1,28.2,2.60])",
      "(5,[0,0,0,1,0,0,1,24.7,2.10])",
      "(5,[0,1,0,0,0,0,1,25.7,2.00])",
      "(0,[0,1,0,0,0,0,1,27.8,2.75])",
      "(3,[0,0,1,0,0,0,1,27.0,2.45])",
      "(10,[0,1,0,0,0,0,1,29.0,3.20])",
      "(7,[0,0,1,0,0,0,1,25.6,2.80])",
      "(0,[0,0,1,0,0,0,1,24.2,1.90])",
      "(0,[0,0,1,0,0,0,1,25.7,1.20])",
      "(0,[0,0,1,0,0,0,1,23.1,1.65])",
      "(0,[0,1,0,0,0,0,1,28.5,3.05])",
      "(5,[0,1,0,0,0,0,1,29.7,3.85])",
      "(0,[0,0,1,0,0,0,1,23.1,1.55])",
      "(1,[0,0,1,0,0,0,1,24.5,2.20])",
      "(1,[0,1,0,0,0,0,1,27.5,2.55])",
      "(1,[0,1,0,0,0,0,1,26.3,2.40])",
      "(3,[0,1,0,0,0,0,1,27.8,3.25])",
      "(2,[0,1,0,0,0,0,1,31.9,3.33])",
      "(5,[0,1,0,0,0,0,1,25.0,2.40])",
      "(0,[0,0,1,0,0,0,1,26.2,2.22])",
      "(3,[0,0,1,0,0,0,1,28.4,3.20])",
      "(6,[0,0,0,1,0,1,0,24.5,1.95])",
      "(7,[0,1,0,0,0,0,1,27.9,3.05])",
      "(6,[0,1,0,0,0,1,0,25.0,2.25])",
      "(3,[0,0,1,0,0,0,1,29.0,2.92])",
      "(4,[0,1,0,0,0,0,1,31.7,3.73])",
      "(4,[0,1,0,0,0,0,1,27.6,2.85])",
      "(0,[0,0,0,1,0,0,1,24.5,1.90])",
      "(0,[0,0,1,0,0,0,1,23.8,1.80])",
      "(8,[0,1,0,0,0,0,1,28.2,3.05])",
      "(0,[0,0,1,0,0,0,1,24.1,1.80])",
      "(0,[0,0,0,1,0,0,1,28.0,2.62])",
      "(9,[0,0,0,1,0,0,1,26.0,2.30])",
      "(0,[0,0,1,0,0,1,0,24.7,1.90])",
      "(0,[0,1,0,0,0,0,1,25.8,2.65])",
      "(8,[0,0,0,1,0,0,1,27.1,2.95])",
      "(5,[0,1,0,0,0,0,1,27.4,2.70])",
      "(2,[0,0,1,0,0,0,1,26.7,2.60])",
      "(5,[0,1,0,0,0,0,1,26.8,2.70])",
      "(0,[0,0,0,1,0,0,1,25.8,2.60])",
      "(0,[0,0,0,1,0,0,1,23.7,1.85])",
      "(6,[0,1,0,0,0,0,1,27.9,2.80])",
      "(5,[0,1,0,0,0,0,1,30.0,3.30])",
      "(4,[0,1,0,0,0,0,1,25.0,2.10])",
      "(5,[0,1,0,0,0,0,1,27.7,2.90])",
      "(15,[0,1,0,0,0,0,1,28.3,3.00])",
      "(0,[0,0,0,1,0,0,1,25.5,2.25])",
      "(5,[0,1,0,0,0,0,1,26.0,2.15])",
      "(0,[0,1,0,0,0,0,1,26.2,2.40])",
      "(1,[0,0,1,0,0,0,1,23.0,1.65])",
      "(0,[0,1,0,0,0,1,0,22.9,1.60])",
      "(5,[0,1,0,0,0,0,1,25.1,2.10])",
      "(4,[0,0,1,0,0,0,1,25.9,2.55])",
      "(0,[0,0,0,1,0,0,1,25.5,2.75])",
      "(0,[0,1,0,0,0,0,1,26.8,2.55])",
      "(1,[0,1,0,0,0,0,1,29.0,2.80])",
      "(1,[0,0,1,0,0,0,1,28.5,3.00])",
      "(4,[0,1,0,0,0,1,0,24.7,2.55])",
      "(1,[0,1,0,0,0,0,1,29.0,3.10])",
      "(6,[0,1,0,0,0,0,1,27.0,2.50])",
      "(0,[0,0,0,1,0,0,1,23.7,1.80])",
      "(6,[0,0,1,0,0,0,1,27.0,2.50])",
      "(2,[0,1,0,0,0,0,1,24.2,1.65])",
      "(4,[0,0,0,1,0,0,1,22.5,1.47])",
      "(0,[0,1,0,0,0,0,1,25.1,1.80])",
      "(0,[0,1,0,0,0,0,1,24.9,2.20])",
      "(6,[0,1,0,0,0,0,1,27.5,2.63])",
      "(0,[0,1,0,0,0,0,1,24.3,2.00])",
      "(4,[0,1,0,0,0,0,1,29.5,3.02])",
      "(0,[0,1,0,0,0,0,1,26.2,2.30])",
      "(4,[0,1,0,0,0,0,1,24.7,1.95])",
      "(4,[0,0,1,0,0,1,0,29.8,3.50])",
      "(0,[0,0,0,1,0,0,1,25.7,2.15])",
      "(2,[0,0,1,0,0,0,1,26.2,2.17])",
      "(0,[0,0,0,1,0,0,1,27.0,2.63])",
      "(0,[0,0,1,0,0,0,1,24.8,2.10])",
      "(0,[0,1,0,0,0,0,1,23.7,1.95])",
      "(11,[0,1,0,0,0,0,1,28.2,3.05])",
      "(1,[0,1,0,0,0,0,1,25.2,2.00])",
      "(4,[0,1,0,0,0,1,0,23.2,1.95])",
      "(3,[0,0,0,1,0,0,1,25.8,2.00])",
      "(0,[0,0,0,1,0,0,1,27.5,2.60])",
      "(0,[0,1,0,0,0,1,0,25.7,2.00])",
      "(0,[0,1,0,0,0,0,1,26.8,2.65])",
      "(3,[0,0,1,0,0,0,1,27.5,3.10])",
      "(9,[0,0,1,0,0,0,1,28.5,3.25])",
      "(3,[0,1,0,0,0,0,1,28.5,3.00])",
      "(6,[0,0,0,1,0,0,1,27.4,2.70])",
      "(3,[0,1,0,0,0,0,1,27.2,2.70])",
      "(0,[0,0,1,0,0,0,1,27.1,2.55])",
      "(1,[0,1,0,0,0,0,1,28.0,2.80])",
      "(0,[0,1,0,0,0,0,1,26.5,1.30])",
      "(0,[0,0,1,0,0,0,1,23.0,1.80])",
      "(3,[0,0,1,0,0,1,0,26.0,2.20])",
      "(0,[0,0,1,0,0,1,0,24.5,2.25])",
      "(0,[0,1,0,0,0,0,1,25.8,2.30])",
      "(0,[0,0,0,1,0,0,1,23.5,1.90])",
      "(0,[0,0,0,1,0,0,1,26.7,2.45])",
      "(0,[0,0,1,0,0,0,1,25.5,2.25])",
      "(1,[0,1,0,0,0,0,1,28.2,2.87])",
      "(1,[0,1,0,0,0,0,1,25.2,2.00])",
      "(2,[0,1,0,0,0,0,1,25.3,1.90])",
      "(0,[0,0,1,0,0,0,1,25.7,2.10])",
      "(12,[0,0,0,1,0,0,1,29.3,3.23])",
      "(6,[0,0,1,0,0,0,1,23.8,1.80])",
      "(3,[0,1,0,0,0,0,1,27.4,2.90])",
      "(2,[0,1,0,0,0,0,1,26.2,2.02])",
      "(4,[0,1,0,0,0,0,1,28.0,2.90])",
      "(5,[0,1,0,0,0,0,1,28.4,3.10])",
      "(7,[0,1,0,0,0,0,1,33.5,5.20])",
      "(0,[0,1,0,0,0,0,1,25.8,2.40])",
      "(10,[0,0,1,0,0,0,1,24.0,1.90])",
      "(0,[0,1,0,0,0,0,1,23.1,2.00])",
      "(0,[0,1,0,0,0,0,1,28.3,3.20])",
      "(4,[0,1,0,0,0,0,1,26.5,2.35])",
      "(7,[0,1,0,0,0,0,1,26.5,2.75])",
      "(3,[0,0,1,0,0,0,1,26.1,2.75])",
      "(0,[0,1,0,0,0,1,0,24.5,2.00])")).map(LabeledPointParser.parse).cache()
  }
  
  test("Modeling generated count data with Poisson regression using L-BFGS.") {
    val posReg = new PoissonRegressionWithLBFGS().setIntercept(true)
    posReg.optimizer
      .setNumCorrections(10)
      .setConvergenceTol(1e-6)
      .setMaxNumIterations(50)
      .setRegParam(0.0)

    val numOfFeatures = 10
    val generatedDataSet = PoissonRegressionDataGenerator
      .generatePoissonRegRDD(sc, 500, numOfFeatures, true)

    val prModel = posReg.run(generatedDataSet, Vectors.dense(new Array[Double](numOfFeatures)))
    
    val predictsOnArray = generatedDataSet map { labeledPoint =>
      math rint prModel.predict(labeledPoint.features)
    }

    val predictsOnRDD = prModel predict {generatedDataSet map (_.features)}

    validatePrediction(predictsOnArray.collect(), generatedDataSet.collect())
    validatePrediction(predictsOnRDD.collect(), generatedDataSet.collect())
  }
  
  test("Modeling generated count data with Poisson regression using SGD.") {
    val posReg = new PoissonRegressionWithSGD().setIntercept(true)
    posReg.optimizer
      .setNumIterations(500)
      .setRegParam(0.0)
      .setStepSize(1e-2)
      .setMiniBatchFraction(0.5)

    val numOfFeatures = 4
    val generatedDataSet = PoissonRegressionDataGenerator
      .generatePoissonRegRDD(sc, 500, numOfFeatures, true)

    val prModel = posReg.run(generatedDataSet, Vectors.dense(new Array[Double](numOfFeatures)))
    
    val predicts = generatedDataSet map { labeledPoint =>
      math rint prModel.predict(labeledPoint.features)
    }
    validatePrediction(predicts.collect(), generatedDataSet.collect())
  }

  test("Modeling real-world count data with Poisson regression using L-BFGS.") {
    val posReg = new PoissonRegressionWithLBFGS().setIntercept(true)
    posReg.optimizer
      .setNumCorrections(10)
      .setConvergenceTol(1e-6)
      .setMaxNumIterations(50)
      .setRegParam(.5)
    
    val prModel = posReg.run(realDataSet, Vectors.dense(new Array[Double](9)))
    
    val predicts = realDataSet map { labeledPoint =>
      math rint prModel.predict(labeledPoint.features)
    }
    validatePrediction(predicts.collect(), realDataSet.collect())
  }

  test("Modeling real-world count data with Poisson regression using SGD.") {
    val posReg = new PoissonRegressionWithSGD().setIntercept(true)
    posReg.optimizer
      .setNumIterations(1600)
      .setRegParam(.5)
      .setStepSize(1e-2)
      .setMiniBatchFraction(0.6)
    
    val prModel = posReg.run(realDataSet, Vectors.dense(new Array[Double](9)))
    
    val predicts = realDataSet map { labeledPoint =>
      math rint prModel.predict(labeledPoint.features)
    }
    validatePrediction(predicts.collect(), realDataSet.collect())
  }
}

