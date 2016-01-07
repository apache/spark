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
import org.apache.spark.mllib.nlp.{CRF, CRFModel}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{StructType, ArrayType, StructField}
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
      System.err.println("Usage: ml.CRFExample <modelFile> <featureFile> <testFile>")
      // scalastyle:on println
      System.exit(1)
    }
    val template = args(0)
    val feature = args(1)
    val test = args(2)

    // Creates a Spark context and a SQL context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .set(s"spark.yarn.jar", s"/home/hujiayin/git/spark/yarn/target/spark.yarn.jar")

    val sc = new SparkContext(conf)

    // Loads data
    /*
    val sqlContext = new SQLContext(sc)
    val rowRDD = sc.textFile(template).filter(_.nonEmpty).map(x => Row(Array(x.split("\t"))))
    val itemType = ScalaReflection.schemaFor[Array[String]].dataType
    val fields = Array(StructField("Values", ArrayType(itemType)))
    val schema = StructType(fields)
    val templateDF = sqlContext.createDataFrame(rowRDD, schema)

    val rowRddF = sc.textFile(feature).filter(_.nonEmpty).map(x => Row(Array(x.split("\t"))))
    val itemTypeF = ScalaReflection.schemaFor[Array[String]].dataType
    val fieldsF = Array(StructField("Values", ArrayType(itemTypeF)))
    val schemaF = StructType(fieldsF)
    val featureDF = sqlContext.createDataFrame(rowRddF, schemaF)

    val crf = new ConditionalRandomField()
    val model = crf.train(templateDF, featureDF, sc)
    */
    val rowRDD = sc.textFile(template).filter(_.nonEmpty).map(_.split("\t"))
    val rowRddF = sc.textFile(feature).filter(_.nonEmpty).map(_.split("\t"))

    val crf = new ConditionalRandomField()
    val model = crf.trainRdd(rowRDD, rowRddF, sc)

    val modelPath = "/home/hujiayin/git/CRFConfig/CRFOutput"
    model.save(sc, modelPath)

    if (args(2) != "") {
      val rowRddT = sc.textFile(test).filter(_.nonEmpty).map(_.split("\t"))
      val modelRDD = sc.parallelize(model.load(sc, modelPath).CRFSeries)
      val newResult = CRF.verifyCRF(rowRddT, modelRDD)

      var idx: Int = 0
      var temp: String = ""
      while (idx < newResult.CRFSeries(0).length) {
        temp += newResult.CRFSeries(0)(idx)
        if ((idx + 1) % 2 == 0) {
          // scalastyle:off println
          println(temp)
          // scalastyle:on println
          temp = ""
        }
        idx += 1
      }
    }

    sc.stop()
  }

}
