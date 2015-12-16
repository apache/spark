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
package org.apache.spark.mllib.nlp

import java.io.Serializable
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, ArrayType, StructField}
import org.apache.spark.{SparkConf, SparkContext, Logging, SparkFunSuite}

/**
 * The language source files could be found at
 * http://www.cnts.ua.ac.be/conll2000/chunking/
 */

class CRFTests extends SparkFunSuite with Logging with Serializable {
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("MLlibUnitTest")
  @transient var sc: SparkContext = new SparkContext(conf)
  val dic = Array(Array(
    "gram",
    "U00:%x[-2,0]",
    "U01:%x[-1,0]",
    "U02:%x[0,0]",
    "U03:%x[1,0]",
    "U04:%x[2,0]",
    "U05:%x[-1,0]/%x[0,0]",
    "U06:%x[0,0]/%x[1,0]",
    "U10:%x[-2,1]",
    "U11:%x[-1,1]",
    "U12:%x[0,1]",
    "U13:%x[1,1]",
    "U14:%x[2,1]",
    "U15:%x[-2,1]/%x[-1,1]",
    "U16:%x[-1,1]/%x[0,1]",
    "U17:%x[0,1]/%x[1,1]",
    "U18:%x[1,1]/%x[2,1]",
    "U20:%x[-2,1]/%x[-1,1]/%x[0,1]",
    "U21:%x[-1,1]/%x[0,1]/%x[1,1]",
    "U22:%x[0,1]/%x[1,1]/%x[2,1]",
    "# Bigram",
    "B")
  )

  val template = sc.parallelize(dic)

  val file = Array(Array(
    "He|PRP|B-NP",
    "reckons|VBZ|B-VP",
    "the|DT|B-NP",
    "current|JJ|I-NP",
    "account|NN|I-NP",
    "deficit|NN|I-NP",
    "will|MD|B-VP",
    "narrow|VB|I-VP",
    "to|TO|B-PP",
    "only|RB|B-NP",
    "#|#|I-NP",
    "1.8|CD|I-NP",
    "billion|CD|I-NP",
    "in|IN|B-PP",
    "September|NNP|B-NP",
    ".|.|O"
  ))

  val modelPath = "~/CRFConfig/CRFModel"
  val resultPath = "~/CRFConfig/CRFResult"
  val src = sc.parallelize(file).cache()
  val CRFModel = CRF.runCRF(template, src)
  CRFModel.save(sc,modelPath)
  val modelRDD = sc.parallelize(CRFModel.load(sc,modelPath).CRFSeries)
  val result = CRF.verifyCRF(src, modelRDD)
  result.save(sc,resultPath)
  var idx: Int = 0
  var temp: String = ""
  println("Word|WordCategory|Label|PredictiveLabel")
  println("---------------------------------------")
  while(idx < result.CRFSeries(0).length) {
    temp += result.CRFSeries(0)(idx)
    if((idx + 1) % 4 == 0) {
      println(temp)
      temp = ""
    }
    idx += 1
  }
}
