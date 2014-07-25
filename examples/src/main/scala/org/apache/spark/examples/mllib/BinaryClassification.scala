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
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{LazyL1Updater, LazySquaredL2Updater, SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import java.util.Random

/**
 * An example app for binary classification. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.BinaryClassification
 * }}}
 * A synthetic dataset is located at `data/mllib/sample_binary_classification_data.txt`.
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

  import Algorithm._
  import RegType._

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
      // (0.0 to 1.0) is minibatch learning, 0.0 is per instance, 1.0 is batch
      miniBatchFraction: Double = 1.0)

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
      opt[Double]("miniBatchFraction")
        .text(
          """
            |        sample fraction per gradient descent step
            |          1.0 is batch
            |          0.0 is stochastic
            |          (0.0 to 1.0) is minibatch
          """.stripMargin)
        .action((x, c) => c.copy(miniBatchFraction = x))
      arg[String]("<input>")
        .required()
        .text("input paths to labeled examples in LIBSVM format")
        .action((x, c) => c.copy(input = x))
      arg[String]("<model>")
          .required()
          .text("path for model")
          .action((x, c) => c.copy(model = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"BinaryClassification with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()
    val perInstanceLearning = params.miniBatchFraction == 0.0
    val (training, test) = if (params.test) {
      (sc.emptyRDD[LabeledPoint], examples)
    } else {
      val splits = examples.randomSplit(Array(0.8, 0.2))

      val reordered = if (perInstanceLearning) {
        // randomize the ordering of training data
        val rand = new Random()
        splits(0).map(x => (rand.nextInt, x)).sortByKey(true).map { _._2 }
      } else splits(0)
      val training = reordered.cache()

      val test = splits(1).cache()
      (training, test)
    }

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    if (!params.test) {
      examples.unpersist(blocking = false)
    }

    val model = if (params.test) {
      // read model
      val strModel = sc.textFile(params.model).collect()
      params.algorithm match {
        case LR =>
          new LogisticRegressionWithSGD().deserializeModel(strModel).clearThreshold()
        case SVM =>
          new SVMWithSGD().deserializeModel(strModel).clearThreshold()
      }
    } else {
      // training setup
      val updater = params.regType match {
        case L1 => if (perInstanceLearning) {
          new LazyL1Updater()
        } else {
          new L1Updater()
        }
        case L2 => if (perInstanceLearning) {
          new LazySquaredL2Updater()
        } else {
          new SquaredL2Updater()
        }
      }

      params.algorithm match {
        case LR =>
          val algorithm = new LogisticRegressionWithSGD()
              .setIntercept(params.addIntercept)
          algorithm.optimizer
              .setNumIterations(params.numIterations)
              .setStepSize(params.stepSize)
              .setUpdater(updater)
              .setRegParam(params.regParam)
              .setMiniBatchFraction(params.miniBatchFraction)
          val model = algorithm.run(training).clearThreshold()
          sc.makeRDD(algorithm.serializeModel(model)).saveAsTextFile(params.model)
          model
        case SVM =>
          val algorithm = new SVMWithSGD()
              .setIntercept(params.addIntercept)
          algorithm.optimizer
              .setNumIterations(params.numIterations)
              .setStepSize(params.stepSize)
              .setUpdater(updater)
              .setRegParam(params.regParam)
              .setMiniBatchFraction(params.miniBatchFraction)
          val model = algorithm.run(training).clearThreshold()
          sc.makeRDD(algorithm.serializeModel(model)).saveAsTextFile(params.model)
          model
      }
    }

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    sc.stop()
  }
}
