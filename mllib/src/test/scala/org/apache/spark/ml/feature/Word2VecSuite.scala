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

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{Row, SQLContext}

class Word2VecSuite extends FunSuite with MLlibTestSparkContext {

  test("Word2Vec") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val localDoc = Seq(sentence, sentence)
    val doc = sc.parallelize(localDoc)
      .map(line => line.split(" "))
    val docDF = doc.map(text => Tuple1(text)).toDF("text")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setSeed(42L)
      .setInputCol("text")
      .setMaxIter(1)
      .fit(docDF)

    val words = sc.parallelize(Seq("a", "b", "c"))
    val codes = Map(
      "a" -> Vectors.dense(-0.2811822295188904,-0.6356269121170044,-0.3020961284637451),
      "b" -> Vectors.dense(1.0309048891067505,-1.29472815990448,0.22276712954044342),
      "c" -> Vectors.dense(-0.08456747233867645,0.5137411952018738,0.11731560528278351)
    )

    val synonyms = Map(
      "a" -> Map("b" -> 0.3680490553379059),
      "b" -> Map("a" -> 0.3680490553379059),
      "c" -> Map("b" -> -0.8148014545440674)
    )
    val wordsDF = words.map(word => Tuple3(word, codes(word), synonyms(word)))
      .toDF("word", "realCode", "realSynonyms")

    val res = model
      .setInputCol("word")
      .setCodeCol("code")
      .setSynonymsCol("syn")
      .setNumSynonyms(1)
      .transform(wordsDF)

    assert(
      res.select("code", "realCode")
        .map { case Row(c: Vector, rc: Vector) => (c, rc) }
        .collect()
        .forall { case (vector1, vector2) =>
          vector1 ~== vector2 absTol 1E-5
        }, "The code is not correct after transforming."
    )

    assert(
      res.select("syn", "realSynonyms")
        .map { case Row(s: Map[String, Double], rs: Map[String, Double]) => (s, rs) }
        .collect()
        .forall { case (map1, map2) =>
          map1 == map2
        }, "The synonyms are not correct after transforming."
    )
  }
}

