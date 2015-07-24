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

package org.apache.spark.examples.ml

import scopt.OptionParser

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.examples.mllib.AbstractParams


/**
 * An example demonstrating a k-means clustering.
 * Run with
 * {{{
 * bin/run-example ml.KMeansExample [options] <file>
 * }}}
 */
object KMeansExample {

  final val FEATURES_COL = "features"

  case class Params(
      input: String = null,
      k: Int = 2,
      maxIter: Int = 20,
      runs: Int = 1,
      epsilon: Double = 1e-4,
      seed: Long = 1,
      initMode: String = MLlibKMeans.K_MEANS_PARALLEL,
      initSteps: Int = 5) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val parsedParams = parseArgs(args)
    parsedParams.map { params =>
      run(params)
    }.getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName} with $params")
    val sc = new SparkContext(conf)

    val dataset = loadData(sc, params.input)

    val kmeans = new KMeans()
      .setK(params.k)
      .setMaxIter(params.maxIter)
      .setRuns(params.runs)
      .setEpsilon(params.epsilon)
      .setSeed(params.seed)
      .setInitMode(params.initMode)
      .setInitSteps(params.initSteps)
      .setFeaturesCol(FEATURES_COL)
    val model = kmeans.fit(dataset)

    // scalastyle:off println
    println("Final Centers: ")
    model.clusterCenters.foreach(println)
    // scalastyle:on println

    sc.stop()
  }

  private def parseArgs(args: Array[String]): Option[Params] = {
    val defaults = Params()
    val parser = new OptionParser[Params]("KMeansExample") {
      head("KMeansExample: an example KMeans.")
      opt[Int](s"k")
        .text("number of clusters created (>= 2). Default: ${defaults.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations (>= 0), default: ${defaults.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Int]("runs")
        .text(s"number of runs of the algorithm to execute in parallel, default: ${defaults.runs}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("epsilon")
        .text(s"distance threshold within which we've consider centers to have converge, " +
          s"default: ${defaults.epsilon}")
        .action((x, c) => c.copy(epsilon = x))
      opt[Long]("seed")
        .text(s"random seed, default: ${defaults.seed}")
        .action((x, c) => c.copy(seed = x))
      opt[String]("initMode")
        .text(s"initialization algorithm " +
          s"(${MLlibKMeans.RANDOM} or ${MLlibKMeans.K_MEANS_PARALLEL}}), " +
          s"default: ${defaults.initMode}")
        .action((x, c) => c.copy(initMode = x))
      opt[String]("initSteps")
        .text(s"number of steps for k-means||, default: ${defaults.initSteps}")
        .action((x, c) => c.copy(initMode = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }
    parser.parse(args, defaults)
  }

  private def loadData(sc: SparkContext, input: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import org.apache.spark.mllib.linalg.VectorUDT

    val rowRDD = sc.textFile(input).filter(l => l != "")
      .map(_.split(" ").map(v => java.lang.Double.parseDouble(v)))
      .map(v => Vectors.dense(v)).map(Row(_))
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    sqlContext.createDataFrame(rowRDD, schema)
  }

}
