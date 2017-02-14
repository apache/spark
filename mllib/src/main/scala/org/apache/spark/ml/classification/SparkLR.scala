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

// scalastyle:off
package org.apache.spark.ml.classification

import java.util.Random

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Logistic regression based classification.
 * Usage: SparkLR [slices]
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to SLogisticRegression.
 */

object SparkLR {

  def parseArgs(args: Array[String]): Map[String, String] = {
    val argsMap = new scala.collection.mutable.HashMap[String, String]
    args.foreach(s => {
      val str = s.trim
      val pos = str.indexOf(":")
      if (pos != -1) {
        val k = str.substring(0, pos)
        val v = str.substring(pos + 1)
        argsMap.put(k, v)
        println(s"args: $k -> $v")
      }
    })
    argsMap.toMap
  }

  def generateData(sampleNum: Int, featLength: Int, partitionNum: Int): Dataset[LabeledPoint] = {

    def randVector(rand: Random, dim: Int): Vector = {
     Vectors.dense(Array.tabulate(dim)(i => rand.nextGaussian()))
    }

    val spark = SparkSession.builder().getOrCreate()

    val rand = new Random(42)
    val initModel = randVector(rand, featLength)
    println(s"init model: ${initModel.toArray.slice(0, 100).mkString(" ")}")

    val bcModel = spark.sparkContext.broadcast(initModel)

    import spark.implicits._
    spark.sparkContext.parallelize(0 until sampleNum, partitionNum)
      .map { id =>
        val rand = new Random(id)
        val feat = randVector(rand, featLength)

        val margin = BLAS.dot(feat, bcModel.value)
        val prob = 1.0 / (1.0 + math.exp(-1 * margin))

        val label = if (rand.nextDouble() > prob) 0.0 else 1.0
        LabeledPoint(label, feat)
      }.toDS()
  }

  def loadData(input: String, partitionNum: Int): Dataset[LabeledPoint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    spark.sparkContext.textFile(input)
      .repartition(partitionNum)
      .map { line =>
        val items = line.split(",")
        val label = items(0).toDouble
        val feat = Vectors.dense(items.tail.map(_.toDouble))
        LabeledPoint(label, feat)
      }.toDS
  }


  def main(args: Array[String]) {
    val argsMap = parseArgs(args)

    val mode = argsMap.getOrElse("mode", "yarn-cluster")
    val input = argsMap.getOrElse("input", null)
    val featNum = argsMap.getOrElse("featNum", "5").toInt
    val sampleNum = argsMap.getOrElse("sampleNum", "1000").toInt
    val partitionNum = argsMap.getOrElse("partitionNum", "10").toInt

    // algo parameter
    val solver = argsMap.getOrElse("solver", "sgd")
    val regParam = argsMap.getOrElse("regParam", "0.001").toDouble
    val maxIter = argsMap.getOrElse("maxIter", "20").toInt
    val tol = argsMap.getOrElse("tol", "0.001").toDouble

    val elasticNet = argsMap.getOrElse("elasticNet", "0.0").toDouble

    // admm
    val rho = argsMap.getOrElse("rho", "0.0001").toDouble
    val numSubModels = argsMap.getOrElse("numSubModels", "20").toInt
    // lbfgs
    val correctionNum = argsMap.getOrElse("correctionNum", "7").toInt
    // sgd
    val stepSize = argsMap.getOrElse("stepSize", "1.0").toDouble
    val batchSize = argsMap.getOrElse("batchSize", "0.3").toDouble

    val spark = SparkSession
      .builder
      .appName("SparkLR")
      .master(mode)
      .getOrCreate()

    val trainSet = loadData(input, partitionNum)

    val lor = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(regParam)
      .setElasticNetParam(elasticNet)
      .setMaxIter(maxIter)

    val lorModel = lor.train(trainSet)
    println(s"trained model: ${lorModel.coefficientMatrix.toString(100, 1000)}")

    spark.stop()
  }
}

// scalastyle:on
