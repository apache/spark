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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}


class DictVectorizerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new DictVectorizer)
    ParamsSuite.checkParams(new DictVectorizer("dict"))
    val model = new DictVectorizerModel("dict", Array("a", "b"))
    val modelWithoutUid = new DictVectorizerModel(Array("a", "b"))
    ParamsSuite.checkParams(model)
    ParamsSuite.checkParams(modelWithoutUid)
  }
  test("DictVectorizer read/write") {
    val t = new DictVectorizer()
      .setInputCols(Array("myInputCol"))
      .setOutputCol("myOutputCol")
      .setHandleInvalid("skip")
      .setSepToken("+")
    testDefaultReadWrite(t)
  }

  test("DictVectorizerModel read/write") {
    val instance = new DictVectorizerModel("myStringIndexerModel", Array("a", "b", "c"))
      .setInputCols(Array("myInputCol"))
      .setOutputCol("myOutputCol")
      .setHandleInvalid("skip")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.vocabulary === instance.vocabulary)
    assert(newInstance.vocabDimension === instance.vocabDimension)

  }
  test("DictVectorizer fits") {
    val data = Seq(
      (0, true, "a", 1.0, Seq("x", "xx"), Seq(true, false), Vector(1.0), Vector(1, 0, 1, 0)),
      (1, false, "b", 1.0, Seq("x"), Seq(true, false), Vector(1.0), Vector(1, 0, 1, 1)),
      (2, true, "c", 1.0, Seq("x", "xx"), Seq(true, false), Vector(1.0), Vector(1, 1, 0)),
      (3, true, "a", 1.0, Seq("x", "xx"), Seq(true, false), Vector(1.0), Vector(1, 0, 1, 0)),
      (4, false, "a", 1.0, Seq("x", "xx"), Seq(true, false), Vector(1.0), Vector(1)),
      (5, true, "c", 1.0, Seq("x", "xx"), Seq(true, false), Vector(1.0), Vector(1)))
//
    val df = data.toDF("id", "bool", "label", "name", "hobbies", "x", "v", "vec")


    val vec = new DictVectorizer()
      .setInputCols(Array("label", "bool", "id", "name", "x", "hobbies", "vec", "v"))
      .setOutputCol("labelIndex")
      .fit(df)

    // copied model must have the same parent.

    MLTestingUtils.checkCopy(vec)

    assert(vec.vocabulary.sorted === Array("id", "bool", "x", "v", "label=a", "label=c",
      "label=b", "name", "hobbies=x", "hobbies=xx", "vec").sorted)
  }

  test("DictVectorizer transforms") {
      val data = Seq(
        (0, true, 1.0, "a", "x", Seq("x", "xx"), Seq(1.0, 2.0), Vector(1, 0, 1, 0)),
        (0, true, 1.0, "a", "x", Seq("x", "xx"), Seq(1.0, 2.0), Vector(1, 0, 1, 0)),
        (0, true, 1.0, "a", "x", Seq("x", "xx"), Seq(1.0, 2.0), Vector(1, 0, 1, 0)),
        (0, true, 1.0, "a", "x", Seq("x", "xx"), Seq(1.0, 2.0), Vector(1, 0, 1, 0)))

      val df = data.toDF("id", "bool", "f", "label", "name", "hobbies", "is", "vec")
      val vec = new DictVectorizer()
        .setInputCols(Array("label", "bool", "f", "id", "name", "hobbies", "is", "vec"))
        .setOutputCol("labelIndex")
        .fit(df)

    val transformed = vec.transform(df)

    transformed.select("labelIndex").collect().foreach {
      case Row(v: Vector) =>
        assert(v === Vectors.sparse(13, Array(0, 1, 2, 3, 5, 6, 7, 8, 9, 11),
          Array(1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0)))
      }
    }
}

