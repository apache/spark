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
import org.scalatest.FunSuite
import org.apache.spark.mllib.util.{LocalSparkContext, MLUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater, HingeGradient}
import org.apache.spark.mllib.classification.SVMWithSGD

/**
 * Created by kellrott on 7/1/14.
 */
class GroupedTestSuite extends FunSuite with LocalSparkContext {

  test("Grouped Optimization") {

    val data = MLUtils.loadLibSVMFile(sc, "data/sample_libsvm_data.txt")
    val folds = MLUtils.kFold(data, 10, 11)
    //SVMWithSGD.train(folds(0)._1, 100)

    val numFeatures: Int = data.first().features.size
    val initialWeights = Vectors.dense(new Array[Double](numFeatures))

    val gradient = new HingeGradient()
    val updater = new SquaredL2Updater()

    val optimizer = new GradientDescent(gradient, updater)
    optimizer.setNumIterations(10)

    val array_out = folds.zipWithIndex.map( f => (f._2, optimizer.optimize( f._1._1.map( x => (x.label, x.features) ), initialWeights ) ) )

    val group_optimizer = new GroupedGradientDescent(gradient, updater)
    group_optimizer.setNumIterations(10)
    val group_train = sc.union(folds.zipWithIndex.map( x => x._1._1.map( y => (x._2, (y.label, y.features)) ) ))
    val group_initialWeights = group_train.keys.collect.map( x => (x,Vectors.dense(new Array[Double](numFeatures))) ).toMap
    val group_out = group_optimizer.optimize( group_train, group_initialWeights )

    //Make sure that each grouped based vector has a match from the
    assert( group_out.map( x => array_out.map( y => x == y).exists(_==true) ).forall(_==true) )

  }

  test("Grouped SVM") {
    val data = MLUtils.loadLibSVMFile(sc, "data/sample_libsvm_data.txt")
    val folds = MLUtils.kFold(data, 10, 11)

    val array_models = folds.map( x => SVMWithSGD.train(x._1, 10))

    val trainingFolds = GroupedUtils.kFoldTrainingUnion(folds)
    val grouped_models = GroupedSVMWithSGD.train(trainingFolds, 10)


    assert( grouped_models.map( x => array_models.map( y => x._2.weights == y.weights).exists(_==true) ).forall(_==true) )

    /*
    array_models.foreach( x => {
      println(x.weights)
    } )
    println("===")
    grouped_models.foreach( x => {
      println(x._2.weights)
    } )
    */
  }

}
