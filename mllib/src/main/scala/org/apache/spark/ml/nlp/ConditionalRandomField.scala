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

package org.apache.spark.ml.nlp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.nlp.{CRF, CRFModel}
import org.apache.spark.sql.{Row, DataFrame}

import scala.collection.mutable.ArrayBuffer

class ConditionalRandomField {
  def train(template: DataFrame,
            sentences: DataFrame,
            sc: SparkContext): CRFModel = {

    val tArr: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()
    val sArr: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()

    template.foreach{row =>
      val t: ArrayBuffer[String] = new ArrayBuffer[String]()
      t.append(row.toString())
      println(row.toString())
      tArr.append(t.toArray)
    }
    val tRdd = sc.parallelize(tArr)

    sentences.foreach{row =>
      val s: ArrayBuffer[String] = new ArrayBuffer[String]()
      s.append(row.toString())
      sArr.append(s.toArray)
    }

    val sRdd = sc.parallelize(tArr)
    val model = CRF.runCRF(tRdd, sRdd)
    model
  }

  def verify(sentences: DataFrame,
             modelExp: DataFrame,
             sc: SparkContext): CRFModel = {
    val md = modelExp.select("*").map { case Row(exp: Array[String]) => exp }
    val src = sentences.select().map { case Row(s: Array[String]) => s }
    val result = CRF.verifyCRF(src, md)
    result
  }

  def trainRdd(template: RDD[Array[String]],
               sentences: RDD[Array[String]],
               sc: SparkContext ): CRFModel = {
    val model = CRF.runCRF(template, sentences)
    model
  }

  def verifyRdd(src: RDD[Array[String]],
                md: RDD[Array[String]],
                sc: SparkContext ): CRFModel = {
    val result = CRF.verifyCRF(src, md)
    result
  }

}
