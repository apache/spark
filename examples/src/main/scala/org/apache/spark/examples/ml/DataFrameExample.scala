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

import java.io.File

import com.google.common.io.Files
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * An example of how to use [[org.apache.spark.sql.DataFrame]] for ML. Run with
 * {{{
 * ./bin/run-example ml.DataFrameExample [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DataFrameExample {

  case class Params(input: String = "data/mllib/sample_libsvm_data.txt")
    extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DataFrameExample") {
      head("DataFrameExample: an example app using DataFrame for ML.")
      opt[String]("input")
        .text(s"input path to dataframe")
        .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        success
      }
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {

    val conf = new SparkConf().setAppName(s"DataFrameExample with $params")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Load input data
    println(s"Loading LIBSVM file with UDT from ${params.input}.")
    val df: DataFrame = sqlContext.read.format("libsvm").load(params.input).cache()
    println("Schema from LIBSVM:")
    df.printSchema()
    println(s"Loaded training data as a DataFrame with ${df.count()} records.")

    // Show statistical summary of labels.
    val labelSummary = df.describe("label")
    labelSummary.show()

    // Convert features column to an RDD of vectors.
    val features = df.select("features").map { case Row(v: Vector) => v }
    val featureSummary = features.aggregate(new MultivariateOnlineSummarizer())(
      (summary, feat) => summary.add(feat),
      (sum1, sum2) => sum1.merge(sum2))
    println(s"Selected features column with average values:\n ${featureSummary.mean.toString}")

    // Save the records in a parquet file.
    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    val outputDir = new File(tmpDir, "dataframe").toString
    println(s"Saving to $outputDir as Parquet file.")
    df.write.parquet(outputDir)

    // Load the records back.
    println(s"Loading Parquet file with UDT from $outputDir.")
    val newDF = sqlContext.read.parquet(outputDir)
    println(s"Schema from Parquet:")
    newDF.printSchema()

    sc.stop()
  }
}
// scalastyle:on println
