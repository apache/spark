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

import org.apache.spark.ml.nlp.ConditionalRandomField
import org.apache.spark.mllib.linalg.{VectorUDT}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * An example demonstrating a CRF.
 * Run with
 * {{{
 * bin/run-example ml.ConditionalRandomFieldExample <modelFile> <featureFile>
 * }}}
 */

object ConditionalRandomFieldExample {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      // scalastyle:off println
      System.err.println("Usage: ml.CRFExample <modelFile> <featureFile>")
      // scalastyle:on println
      System.exit(1)
    }
    val template = args(0)
    val feature = args(1)

    // Creates a Spark context and a SQL context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Loads data
    var rowRDD = sc.textFile(template).filter(_.nonEmpty).map(Row(_))
    var schema = StructType(Array(StructField("", new VectorUDT, false)))
    val templateDF = sqlContext.createDataFrame(rowRDD, schema)

    rowRDD = sc.textFile(feature).filter(_.nonEmpty).map(Row(_))
    schema = StructType(Array(StructField("", new VectorUDT, false)))
    val featureDF = sqlContext.createDataFrame(rowRDD, schema)

    val crf = new ConditionalRandomField()
    val model = crf.train(templateDF, featureDF)

    // Shows the result
    // scalastyle:off println
    println("CRF expectations:")
    model.CRFSeries.foreach(println)
    // scalastyle:on println

    sc.stop()
  }

}
