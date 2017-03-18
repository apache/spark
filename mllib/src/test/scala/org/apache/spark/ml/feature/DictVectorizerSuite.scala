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
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext

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

  test("DictVectorizer") {
    val data = Seq(
      (0, "a", "x", Seq("x", "xx")),
      (1, "b", "x", Seq("x")),
      (2, "c", "x", Seq("x", "xx")),
      (3, "a", "x", Seq("x", "xx")),
      (4, "a", "x", Seq("x", "xx")),
      (5, "c", "x", Seq("x", "xx")))

    val df = data.toDF("id", "label", "name", "hobbies")
    val indexer = new DictVectorizer()
      .setInputCols(Array("label", "id", "name", "hobbies"))
      .setOutputCol("labelIndex")
      .fit(df)

    // copied model must have the same parent.
    // scalastyle:off

    MLTestingUtils.checkCopy(indexer)

    val transformed = indexer.transform(df)

    val attr = Attribute.fromStructField(transformed.schema("labelIndex"))
      .asInstanceOf[NominalAttribute]
    assert(attr.values.get === Array("a", "c", "b", "x"))
    val output = transformed.select("id", "labelIndex").rdd.map { r =>
      (r.getInt(0), r.getDouble(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0))
    assert(output === expected)
  }
}

