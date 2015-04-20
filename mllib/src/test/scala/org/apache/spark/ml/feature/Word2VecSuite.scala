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

package org.apache.spark.ml.feature

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector, SparseVector}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class Word2VecSuite extends FunSuite with MLlibTestSparkContext {

  test("Word2Vec") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val sentence = "a b " * 100 + "a c " * 10
    val localDoc = Seq(sentence, sentence)
    val doc = sc.parallelize(localDoc)
      .map(line => line.split(" ").toSeq)
    val docDF = doc.map(text => Tuple1(text)).toDF("text")
    val model = new Word2Vec().setVectorSize(10).setSeed(42L).setInputCol("text").setMaxIter(1).fit(docDF)
    val words = sc.parallelize(Seq("a", "b", "c"))
    val wordsDF = words.map(word => Tuple1(word)).toDF("word")
    val res = model.setInputCol("word").setCodeCol("code").setSynonymsCol("syn").setNumSynonyms(2).transform(wordsDF)
    res.select("word", "code", "syn").foreach { case Row(w: String, c: Vector, s: Map[String, Double]) =>
      println(s"word $w's code is $c")
      println(s"word $w's synonyms are ${s.mkString(", ")}")
    }
  }
}

