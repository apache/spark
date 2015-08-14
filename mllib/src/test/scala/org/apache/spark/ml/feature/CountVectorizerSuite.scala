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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class CountVectorizerSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {
    ParamsSuite.checkParams(new CountVectorizerModel(Array("empty")))
  }

  test("CountVectorizerModel common cases") {
    val df = sqlContext.createDataFrame(Seq(
      (0, "a b c d".split("\\s+").toSeq,
        Vectors.sparse(4, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)))),
      (1, "a b b c d  a".split("\\s+").toSeq,
        Vectors.sparse(4, Seq((0, 2.0), (1, 2.0), (2, 1.0), (3, 1.0)))),
      (2, "a".split("\\s+").toSeq, Vectors.sparse(4, Seq((0, 1.0)))),
      (3, "".split("\\s+").toSeq, Vectors.sparse(4, Seq())), // empty string
      (4, "a notInDict d".split("\\s+").toSeq,
        Vectors.sparse(4, Seq((0, 1.0), (3, 1.0))))  // with words not in vocabulary
    )).toDF("id", "words", "expected")
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
    val output = cv.transform(df).collect()
    output.foreach { p =>
      val features = p.getAs[Vector]("features")
      val expected = p.getAs[Vector]("expected")
      assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer common cases") {
    val df = sqlContext.createDataFrame(Seq(
      (0, "a b c d e".split("\\s+").toSeq,
        Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))),
      (1, "a a a a a a".split("\\s+").toSeq, Vectors.sparse(5, Seq((0, 6.0)))),
      (2, "c".split("\\s+").toSeq, Vectors.sparse(5, Seq((2, 1.0)))),
      (3, "b b b b b".split("\\s+").toSeq, Vectors.sparse(5, Seq((1, 5.0)))))
    ).toDF("id", "words", "expected")
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .fit(df)
    assert(cv.vocabulary.deep == Array("a", "b", "c", "d", "e").deep)

    val output = cv.transform(df).collect()
    output.foreach { p =>
      val features = p.getAs[Vector]("features")
      val expected = p.getAs[Vector]("expected")
      assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer vocabSize and minCount") {
    val df = sqlContext.createDataFrame(Seq(
      (0, "a a a a a".split("\\s+").toSeq, Vectors.sparse(3, Seq((0, 5.0)))),
      (1, "b b b b".split("\\s+").toSeq, Vectors.sparse(3, Seq((1, 4.0)))),
      (2, "c c c".split("\\s+").toSeq, Vectors.sparse(3, Seq((2, 3.0)))),
      (3, "d d".split("\\s+").toSeq, Vectors.sparse(3, Seq())))
    ).toDF("id", "words", "expected")
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)  // limit vocab size to 3
      .fit(df)
    assert(cvModel.vocabulary.deep == Array("a", "b", "c").deep)

    val cvModel2 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinCount(3)  // ignore terms with count less than 3
      .fit(df)
    assert(cvModel2.vocabulary.deep == Array("a", "b", "c").deep)

    val output = cvModel2.transform(df).collect()
    output.foreach { p =>
      val features = p.getAs[Vector]("features")
      val expected = p.getAs[Vector]("expected")
      assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer throws exception when vocab is empty") {
    intercept[IllegalArgumentException] {
      val df = sqlContext.createDataFrame(Seq(
        (0, "a a b b c c".split("\\s+").toSeq),
        (1, "aa bb cc".split("\\s+").toSeq))
      ).toDF("id", "words")
      val cvModel = new CountVectorizer()
        .setInputCol("words")
        .setOutputCol("features")
        .setVocabSize(3)  // limit vocab size to 3
        .setMinCount(3)
        .fit(df)
    }
  }
}


