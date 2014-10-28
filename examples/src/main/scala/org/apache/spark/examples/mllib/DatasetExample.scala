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

import java.io.File

import com.google.common.io.Files
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SchemaRDD}

/**
 * An example of how to use [[org.apache.spark.sql.SchemaRDD]] as a Dataset for ML. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DatasetExample [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DatasetExample {

  case class Params(
      input: String = "data/mllib/sample_libsvm_data.txt",
      dataFormat: String = "libsvm") extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DatasetExample") {
      head("Dataset: an example app using SchemaRDD as a Dataset for ML.")
      opt[String]("input")
        .text(s"input path to dataset")
        .action((x, c) => c.copy(input = x))
      opt[String]("dataFormat")
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
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

    val conf = new SparkConf().setAppName(s"DatasetExample with $params")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._ // for implicit conversions

    // Load input data
    val origData: RDD[LabeledPoint] = params.dataFormat match {
      case "dense" => MLUtils.loadLabeledPoints(sc, params.input)
      case "libsvm" => MLUtils.loadLibSVMFile(sc, params.input)
    }
    println(s"Loaded ${origData.count()} instances from file: ${params.input}")

    // Convert input data to SchemaRDD explicitly.
    val schemaRDD: SchemaRDD = origData
    println(s"Inferred schema:\n${schemaRDD.schema.prettyJson}")
    println(s"Converted to SchemaRDD with ${schemaRDD.count()} records")

    // Select columns, using implicit conversion to SchemaRDD.
    val labelsSchemaRDD: SchemaRDD = origData.select('label)
    val labels: RDD[Double] = labelsSchemaRDD.map { case Row(v: Double) => v }
    val numLabels = labels.count()
    val meanLabel = labels.fold(0.0)(_ + _) / numLabels
    println(s"Selected label column with average value $meanLabel")

    val featuresSchemaRDD: SchemaRDD = origData.select('features)
    val features: RDD[Vector] = featuresSchemaRDD.map { case Row(v: Vector) => v }
    val featureSummary = features.aggregate(new MultivariateOnlineSummarizer())(
      (summary, feat) => summary.add(feat),
      (sum1, sum2) => sum1.merge(sum2))
    println(s"Selected features column with average values:\n ${featureSummary.mean.toString}")

    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    val outputDir = new File(tmpDir, "dataset").toString
    println(s"Saving to $outputDir as Parquet file.")
    schemaRDD.saveAsParquetFile(outputDir)

    println(s"Loading Parquet file with UDT from $outputDir.")
    val newDataset = sqlContext.parquetFile(outputDir)

    println(s"Schema from Parquet: ${newDataset.schema.prettyJson}")
    val newFeatures = newDataset.select('features).map { case Row(v: Vector) => v }
    val newFeaturesSummary = newFeatures.aggregate(new MultivariateOnlineSummarizer())(
      (summary, feat) => summary.add(feat),
      (sum1, sum2) => sum1.merge(sum2))
    println(s"Selected features column with average values:\n ${newFeaturesSummary.mean.toString}")

    sc.stop()
  }

}
