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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class FeatureHasherSuite extends SparkFunSuite
  with MLlibTestSparkContext
  with DefaultReadWriteTest {

  import testImplicits._
  import HashingTFSuite.murmur3FeatureIdx

  test("params") {
    ParamsSuite.checkParams(new FeatureHasher)
  }

  test("specify input cols using varargs or array") {
    val featureHasher1 = new FeatureHasher()
      .setInputCols("int", "double", "float", "stringNum", "string")
    val featureHasher2 = new FeatureHasher()
      .setInputCols(Array("int", "double", "float", "stringNum", "string"))
    assert(featureHasher1.getInputCols === featureHasher2.getInputCols)
  }

  test("feature hashing") {
    val df = Seq(
      (2, 2.0, 2.0f, "2", "foo"),
      (3, 3.0, 3.0f, "3", "bar")
    ).toDF("int", "double", "float", "stringNum", "string")
    val n = 100
    val featureHasher = new FeatureHasher()
      .setInputCols("int", "double", "float", "stringNum", "string")
      .setOutputCol("features")
      .setNumFeatures(n)
    val output = featureHasher.transform(df)
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    require(attrGroup.numAttributes === Some(n))
    val features = output.select("features").as[Vector].collect()
    // Assume perfect hash on field names
    def idx: Any => Int = murmur3FeatureIdx(n)

    // check expected indices
    val expected = Seq(
      Vectors.sparse(n, Seq((idx("int"), 2.0), (idx("double"), 2.0), (idx("float"), 2.0),
        (idx("stringNum=2"), 1.0), (idx("string=foo"), 1.0))),
      Vectors.sparse(n, Seq((idx("int"), 3.0), (idx("double"), 3.0), (idx("float"), 3.0),
        (idx("stringNum=3"), 1.0), (idx("string=bar"), 1.0)))
    )
    assert(features.zip(expected).forall { case (e, a) => e ~== a absTol 1e-14 })
  }

  test("read/write") {
    val t = new FeatureHasher()
      .setInputCols(Array("myCol1", "myCol2", "myCol3"))
      .setOutputCol("myOutputCol")
      .setNumFeatures(10)
    testDefaultReadWrite(t)
  }
}
