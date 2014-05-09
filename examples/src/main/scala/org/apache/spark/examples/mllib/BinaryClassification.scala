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

package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, LogisticRegressionModel, LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.binary.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, GeneralizedLinearModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

/**
 * An example app for binary classification. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.BinaryClassification
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object BinaryClassification {

  object Algorithm extends Enumeration {
    type Algorithm = Value
    val SVM, LR = Value
  }

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }

  object Mode extends Enumeration {
    type Mode = Value
    val TRAIN, TEST, SPLIT = Value
  }

  import Algorithm._
  import RegType._
  import Mode._

  case class Params(
      input: String = null,
      model: String = null,
      numIterations: Int = 100,
      stepSize: Double = 1.0,
      algorithm: Algorithm = LR,
      regType: RegType = L2,
      regParam: Double = 0.1,
      test: Boolean = false,
      addIntercept: Boolean = false,
      stochastic: Boolean = false,
      miniBatch: Double = 1.0,
      prediction: String = null)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("BinaryClassification") {
      head("BinaryClassification: an example app for binary classification.")
      opt[Int]("numIterations")
        .text("number of iterations")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("stepSize")
        .text(s"initial step size, default: ${defaultParams.stepSize}")
        .action((x, c) => c.copy(stepSize = x))
      opt[String]("algorithm")
        .text(s"algorithm (${Algorithm.values.mkString(",")}), " +
        s"default: ${defaultParams.algorithm}")
        .action((x, c) => c.copy(algorithm = Algorithm.withName(x)))
      opt[String]("regType")
        .text(s"regularization type (${RegType.values.mkString(",")}), " +
        s"default: ${defaultParams.regType}")
        .action((x, c) => c.copy(regType = RegType.withName(x)))
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Unit]("test")
          .text("test given model")
          .action((x, c) => c.copy(test = true))
      opt[Unit]("addIntercept")
        .text("add interept 1.0 to input data")
        .action((_, c) => c.copy(addIntercept = true))
      opt[Unit]("stochastic")
        .text("use stochastic gradient descent")
        .action((_, c) => c.copy(stochastic = true))
      opt[Double]("miniBatch")
        .text("sample fraction per gradient descent step")
        .action((x, c) => c.copy(miniBatch = x))
      opt[String]("prediction")
          .text("prediction file")
          .action((x, c) => c.copy(prediction = x))
      arg[String]("<input>")
        .required()
        .text("input paths to labeled examples in LIBSVM format")
        .action((x, c) => c.copy(input = x))
      arg[String]("<model>")
          .required()
          .text("path for model")
          .action((x, c) => c.copy(model = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def parseModel(strModel: Seq[(String, Double)]): (Vector, Double) = {
    val numFeatures = strModel(0)._2.toInt
    val intercept = strModel(1)._2
    val weights = Array.fill(numFeatures) { 0.0d }
    strModel.slice(2, strModel.length).foreach { kv =>
      weights(kv._1.toInt) = kv._2
    }
    (Vectors.dense(weights), intercept)
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"BinaryClassification with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = MLUtils.loadLibSVMData(sc, params.input).cache()

    val (training, test) = if (params.test) {
      (sc.emptyRDD[LabeledPoint], examples)
    } else {
      val splits = examples.randomSplit(Array(0.8, 0.2))
      val training = splits(0).cache()
      val test = splits(1).cache()
      examples.unpersist(blocking = false)
      (training, test)
    }

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    val model = if (params.test) {
      val strModel: Seq[(String, Double)]= sc.textFile(params.model).map { line =>
        val parts = line.split(":")
        (parts(0), parts(1).toDouble)
      } .collect()
      val (weights, intercept) = parseModel(strModel)

      params.algorithm match {
        case LR => new LogisticRegressionModel(weights, intercept)
        case SVM => new SVMModel(weights, intercept)
      }
    } else {
      val updater = params.regType match {
        case L1 => new L1Updater()
        case L2 => new SquaredL2Updater()
      }

      val (algorithm, optimizer) = params.algorithm match {
        case LR =>
          val lr = new LogisticRegressionWithSGD()
          (lr, lr.optimizer)
        case SVM =>
          val svm = new SVMWithSGD()
          (svm, svm.optimizer)
      }
      algorithm.setIntercept(params.addIntercept)
      optimizer
            .setNumIterations(params.numIterations)
            .setStepSize(params.stepSize)
            .setUpdater(updater)
            .setRegParam(params.regParam)
            .setMiniBatchFraction(params.miniBatch)
            .setStochastic(params.stochastic)
      val model = algorithm.run(training)

      sc.makeRDD(model.readableModel).saveAsTextFile(params.model)

      model
    }

    params.algorithm match {
      case LR => model.asInstanceOf[LogisticRegressionModel].clearThreshold()
      case SVM => model.asInstanceOf[SVMModel].clearThreshold()
    }

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    if (params.prediction != null) {
      predictionAndLabel.map { lp => lp._2 + "," + lp._1 }
          .saveAsTextFile(params.prediction)
    }

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    sc.stop()
  }
}
