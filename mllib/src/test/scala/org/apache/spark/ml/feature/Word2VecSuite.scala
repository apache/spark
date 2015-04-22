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
    val numOfWords = sentence.split(" ").size
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(-0.2811822295188904,-0.6356269121170044,-0.3020961284637451),
      "b" -> Array(1.0309048891067505,-1.29472815990448,0.22276712954044342),
      "c" -> Array(-0.08456747233867645,0.5137411952018738,0.11731560528278351)
    )

    val expected = doc.map { sentence =>
      Vectors.dense(sentence.map(codes.apply).reduce((word1, word2) =>
        word1.zip(word2).map { case (v1, v2) => v1 + v2 }
      ).map(_ / numOfWords))
    }

    val docDF = doc.zip(expected).toDF("text", "expected")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .fit(docDF)

    model.transform(docDF).select("result", "expected").collect().foreach {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== vector2 absTol 1E-5, "Transformed vector is different with expected.")
    }
  }
}

