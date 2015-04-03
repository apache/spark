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

package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.classification._

import org.scalatest.{FunSuite, Matchers}

class LRonGraphXSuite extends FunSuite with LocalClusterSparkContext with Matchers {
  test("10M dataSet") {

    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val dataSetFile = s"${sparkHome}/data/mllib/trainingset.10M.txt"
    // val dataSetFile = s"${sparkHome}/data/mllib/kdda.10m.txt"
    // val dataSetFile = s"${sparkHome}/data/mllib/url_combined.10m.txt"
    val dataSet = MLUtils.loadLibSVMFile(sc, dataSetFile)


    //    val dataSetFile = s"/input/lbs/recommend/kdda/*"
    //    val dataSetFile = s"/input/lbs/recommend/url_combined/*"
    //    val dataSet = MLUtils.loadLibSVMFile(sc, dataSetFile).repartition(72)

    val trainSet = dataSet
    val stepSize = 1e-1
    val numIterations = 1000
    val regParam = 0.0
    LRonGraphX.train(trainSet, numIterations, stepSize, regParam)

    //    val trainSet = dataSet.map(t => {
    //      LabeledPoint(if (t.label > 0) 1 else 0, t.features)
    //    }).cache()
    //    val algorithm = new LogisticRegressionWithLBFGS()
    //    algorithm.optimizer.setNumIterations(1000).setUpdater(new L1Updater()).setRegParam(regParam)
    //    algorithm.run(trainSet)

  }
}
