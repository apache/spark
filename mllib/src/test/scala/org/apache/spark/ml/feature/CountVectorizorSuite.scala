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
      (0, "a b c d".split(" ").toSeq,
        Vectors.sparse(4, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)))),
      (1, "a b b c d  a".split(" ").toSeq,
        Vectors.sparse(4, Seq((0, 2.0), (1, 2.0), (2, 1.0), (3, 1.0)))),
      (2, "a".split(" ").toSeq, Vectors.sparse(4, Seq((0, 1.0)))),
      (3, "".split(" ").toSeq, Vectors.sparse(4, Seq())), // empty string
      (4, "a notInDict d".split(" ").toSeq,
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

  test("CountVectorizerModel with minTermFreq") {
    val df = sqlContext.createDataFrame(Seq(
      (0, "a a a b b c c c d ".split(" ").toSeq, Vectors.sparse(4, Seq((0, 3.0), (2, 3.0)))),
      (1, "c c c c c c".split(" ").toSeq, Vectors.sparse(4, Seq((2, 6.0)))),
      (2, "a".split(" ").toSeq, Vectors.sparse(4, Seq())),
      (3, "e e e e e".split(" ").toSeq, Vectors.sparse(4, Seq())))
    ).toDF("id", "words", "expected")
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTermFreq(3)
    val output = cv.transform(df).collect()
    output.foreach { p =>
      val features = p.getAs[Vector]("features")
      val expected = p.getAs[Vector]("expected")
      assert(features ~== expected absTol 1e-14)
    }
  }
}


