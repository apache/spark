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
  val dic = Seq(
    "gram\n" +
    "U00:%x[-2,0]\n" +
    "U01:%x[-1,0]\n" +
    "U02:%x[0,0]\n" +
    "U03:%x[1,0]\n" +
    "U04:%x[2,0]\n" +
    "U05:%x[-1,0]/%x[0,0]\n" +
    "U06:%x[0,0]/%x[1,0]\n" +
    "U10:%x[-2,1]\n" +
    "U11:%x[-1,1]\n" +
    "U12:%x[0,1]\n" +
    "U13:%x[1,1]\n" +
    "U14:%x[2,1]\n" +
    "U15:%x[-2,1]/%x[-1,1]\n" +
    "U16:%x[-1,1]/%x[0,1]\n" +
    "U17:%x[0,1]/%x[1,1]\n" +
    "U18:%x[1,1]/%x[2,1]\n" +
    "U20:%x[-2,1]/%x[-1,1]/%x[0,1]\n" +
    "U21:%x[-1,1]/%x[0,1]/%x[1,1]\n" +
    "U22:%x[0,1]/%x[1,1]/%x[2,1]\n" +
    "# Bigram\n" +
    "B\n"
  )

  val template = sc.parallelize(dic)

  val file = Seq(
    "He|PRP|B-NP\n" +
    "reckons|VBZ|B-VP\n" +
    "the|DT|B-NP\n" +
    "current|JJ|I-NP\n" +
    "account|NN|I-NP\n" +
    "deficit|NN|I-NP\n" +
    "will|MD|B-VP\n" +
    "narrow|VB|I-VP\n" +
    "to|TO|B-PP\n" +
    "only|RB|B-NP\n" +
    "#|#|I-NP\n" +
    "1.8|CD|I-NP\n" +
    "billion|CD|I-NP\n" +
    "in|IN|B-PP\n" +
    "September|NNP|B-NP\n" +
    ".|.|O"
  )
  val src = sc.parallelize(file).cache()


  CRF.runCRF(template, src)

/**
   The model file is the result of CRF train.

    val model = Seq("0.04941300124324025\n",
    "-0.009882600248648099\n" +
    "-0.009882600248648099\n" +
    "-0.009882600248648099\n" +
    "-0.009882600248648099\n" +
    "-0.009882600248648099...")
  val modelRdd = sc.parallelize(model).cache()
  val result = CRF.verifyCRF(src, modelRdd)

  }
 */

}
