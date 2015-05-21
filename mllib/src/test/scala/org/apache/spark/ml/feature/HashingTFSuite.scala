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

import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HashingTFSuite extends FunSuite with MLlibTestSparkContext {

  test("params") {
    val hashingTF = new HashingTF
    ParamsSuite.checkParams(hashingTF, 3)
  }

  test("hashingTF") {
    val df = sqlContext.createDataFrame(Seq(
      (0, "a a b b c d".split(" ").toSeq)
    )).toDF("id", "words")
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(1000)
    val output = hashingTF.transform(df)
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    require(attrGroup.numAttributes === Some(1000))
    val features = output.select("features").first().getAs[Vector](0)
    // Assume perfect hash on "a", "b", "c", and "d".
    val expected = Vectors.sparse(1000,
      Seq(("a".##, 2.0), ("b".##, 2.0), ("c".##, 1.0), ("d".##, 1.0)))
    assert(features ~== expected absTol 1e-14)
  }
}
