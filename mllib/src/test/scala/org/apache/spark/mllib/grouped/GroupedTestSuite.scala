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

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.{LocalSparkContext, MLUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater, HingeGradient}
import org.apache.spark.mllib.classification.{SVMSuite, LogisticRegressionSuite, LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint

import org.scalatest.FunSuite
import org.scalatest.Matchers

class GroupedTestSuite extends FunSuite with LocalSparkContext with Matchers {

  test("Grouped Optimization") {

    val nPoints = 10000
    val A = 0.01
    val B = -1.5
    val C = 1.0
    val testData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 42)
    val data = sc.parallelize(testData)
    val folds = MLUtils.kFold(data, 10, 11)

    val numFeatures: Int = data.first().features.size
    val initialWeights = Vectors.dense(new Array[Double](numFeatures))

    val gradient = new HingeGradient()
    val updater = new SquaredL2Updater()

    val optimizer = new GradientDescent(gradient, updater)
    optimizer.setNumIterations(10)

    val array_out = folds.zipWithIndex.map( f => (f._2, optimizer.optimize( f._1._1.map( x => (x.label, x.features) ), initialWeights ) ) )

    val group_optimizer = new GroupedGradientDescent[Int](gradient, updater)
    group_optimizer.setNumIterations(10)
    val group_train = sc.union(folds.zipWithIndex.map( x => x._1._1.map( y => (x._2, (y.label, y.features)) ) ))
    val group_initialWeights = group_train.keys.collect.map( x => (x,Vectors.dense(new Array[Double](numFeatures))) ).toMap
    val group_out = group_optimizer.optimize( group_train, group_initialWeights )

    //Make sure that each grouped based vector has a match from the
    assert( group_out.map( x => array_out.map( y => x == y).exists(_==true) ).forall(_==true) )

  }

  test("Grouped SVM") {
    val nPoints = 10000
    val A = 0.01
    val B = -1.5
    val C = 1.0
    val testData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 42)
    val data = sc.parallelize(testData)
    val folds = MLUtils.kFold(data, 10, 11)

    val array_models = folds.map( x => SVMWithSGD.train(x._1, 10))

    val trainingFolds = GroupedUtils.kFoldTrainingUnion(folds)
    val grouped_models = GroupedSVMWithSGD.train(trainingFolds, 10)

    assert( grouped_models.map( x => array_models.map( y => x._2.weights == y.weights).exists(_==true) ).forall(_==true) )
  }


  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 83% of the predictions should be on.
    ((input.length - numOffPredictions).toDouble / input.length) should be > 0.83
  }

  // Test if grouped logistic regression method returns the same results as doing
  // regressions one by one
  test("Grouped logistic regression") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val baseTestRDD = sc.parallelize(testData, 2)
    baseTestRDD.cache()

    val testFolds = MLUtils.kFold(baseTestRDD, 10, 42)

    val testUnion = GroupedUtils.kFoldTrainingUnion(testFolds)

    val array_models = testFolds.map( _._1 ).map( testRDD => {
      val lr = new LogisticRegressionWithSGD().setIntercept(true)
      lr.optimizer.setStepSize(10.0).setNumIterations(20)
      lr.run(testRDD)
    } )

    val glr = new GroupedLogisticRegressionWithSGD[Int]().setIntercept(true)
    glr.optimizer.setStepSize(10.0).setNumIterations(20)
    val grouped_models = glr.run(testUnion)

    assert( grouped_models.map( x => array_models.map( y => x._2.weights == y.weights).exists(_==true) ).forall(_==true) )
  }

}
