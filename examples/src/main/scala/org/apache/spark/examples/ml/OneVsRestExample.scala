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

// scalastyle:off println
package org.apache.spark.examples.ml

import java.util.concurrent.TimeUnit.{NANOSECONDS => NANO}

import scopt.OptionParser

import org.apache.spark.{SparkContext, SparkConf}
// $example on$
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
// $example off$
import org.apache.spark.sql.SQLContext

/**
 * An example runner for Multiclass to Binary Reduction with One Vs Rest.
 * The example uses Logistic Regression as the base classifier. All parameters that
 * can be specified on the base classifier can be passed in to the runner options.
 * Run with
 * {{{
 * ./bin/run-example ml.OneVsRestExample [options]
 * }}}
 * For local mode, run
 * {{{
 * ./bin/spark-submit --class org.apache.spark.examples.ml.OneVsRestExample --driver-memory 1g
 *   [examples JAR path] [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object OneVsRestExample {

  case class Params private[ml] (
      input: String = null,
      testInput: Option[String] = None,
      maxIter: Int = 100,
      tol: Double = 1E-6,
      fitIntercept: Boolean = true,
      regParam: Option[Double] = None,
      elasticNetParam: Option[Double] = None,
      fracTest: Double = 0.2) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("OneVsRest Example") {
      head("OneVsRest Example: multiclass to binary reduction using OneVsRest")
      opt[String]("input")
        .text("input path to labeled examples. This path must be specified")
        .required()
        .action((x, c) => c.copy(input = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
        s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("testInput")
        .text("input path to test dataset.  If given, option fracTest is ignored")
        .action((x, c) => c.copy(testInput = Some(x)))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations for Logistic Regression." +
          s" default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("tol")
        .text(s"the convergence tolerance of iterations for Logistic Regression." +
          s" default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))
      opt[Boolean]("fitIntercept")
        .text(s"fit intercept for Logistic Regression." +
        s" default: ${defaultParams.fitIntercept}")
        .action((x, c) => c.copy(fitIntercept = x))
      opt[Double]("regParam")
        .text(s"the regularization parameter for Logistic Regression.")
        .action((x, c) => c.copy(regParam = Some(x)))
      opt[Double]("elasticNetParam")
        .text(s"the ElasticNet mixing parameter for Logistic Regression.")
        .action((x, c) => c.copy(elasticNetParam = Some(x)))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName(s"OneVsRestExample with $params")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val inputData = sqlContext.read.format("libsvm").load(params.input)
    // compute the train/test split: if testInput is not provided use part of input.
    val data = params.testInput match {
      case Some(t) => {
        // compute the number of features in the training set.
        val numFeatures = inputData.first().getAs[Vector](1).size
        val testData = sqlContext.read.option("numFeatures", numFeatures.toString)
          .format("libsvm").load(t)
        Array[DataFrame](inputData, testData)
      }
      case None => {
        val f = params.fracTest
        inputData.randomSplit(Array(1 - f, f), seed = 12345)
      }
    }
    val Array(train, test) = data.map(_.cache())

    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(params.maxIter)
      .setTol(params.tol)
      .setFitIntercept(params.fitIntercept)

    // Set regParam, elasticNetParam if specified in params
    params.regParam.foreach(classifier.setRegParam)
    params.elasticNetParam.foreach(classifier.setElasticNetParam)

    // instantiate the One Vs Rest Classifier.

    val ovr = new OneVsRest()
    ovr.setClassifier(classifier)

    // train the multiclass model.
    val (trainingDuration, ovrModel) = time(ovr.fit(train))

    // score the model on test data.
    val (predictionDuration, predictions) = time(ovrModel.transform(test))

    // evaluate the model
    val predictionsAndLabels = predictions.select("prediction", "label")
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new MulticlassMetrics(predictionsAndLabels)

    val confusionMatrix = metrics.confusionMatrix

    // compute the false positive rate per label
    val predictionColSchema = predictions.schema("prediction")
    val numClasses = MetadataUtils.getNumClasses(predictionColSchema).get
    val fprs = Range(0, numClasses).map(p => (p, metrics.falsePositiveRate(p.toDouble)))

    println(s" Training Time ${trainingDuration} sec\n")

    println(s" Prediction Time ${predictionDuration} sec\n")

    println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")

    println("label\tfpr")

    println(fprs.map {case (label, fpr) => label + "\t" + fpr}.mkString("\n"))
    // $example off$

    sc.stop()
  }

  private def time[R](block: => R): (Long, R) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    (NANO.toSeconds(t1 - t0), result)
  }
}
// scalastyle:on println
