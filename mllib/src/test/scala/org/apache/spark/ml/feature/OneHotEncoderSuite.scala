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
import org.apache.spark.ml.attribute.{AttributeGroup, BinaryAttribute, NominalAttribute}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class OneHotEncoderSuite extends SparkFunSuite with MLlibTestSparkContext {

  def stringIndexed(): DataFrame = {
    val data = sc.parallelize(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")), 2)
    val df = sqlContext.createDataFrame(data).toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    indexer.transform(df)
  }

  test("params") {
    ParamsSuite.checkParams(new OneHotEncoder)
  }

  test("OneHotEncoder dropLast = false") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
      .setDropLast(false)
    val encoded = encoder.transform(transformed)

    val output = encoded.select("id", "labelVec").map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1), vec(2))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0, 0.0), (1, 0.0, 0.0, 1.0), (2, 0.0, 1.0, 0.0),
      (3, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 1.0, 0.0))
    assert(output === expected)
  }

  test("OneHotEncoder dropLast = true") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val encoded = encoder.transform(transformed)

    val output = encoded.select("id", "labelVec").map { r =>
      val vec = r.getAs[Vector](1)
      (r.getInt(0), vec(0), vec(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0), (1, 0.0, 0.0), (2, 0.0, 1.0),
      (3, 1.0, 0.0), (4, 1.0, 0.0), (5, 0.0, 1.0))
    assert(output === expected)
  }

  test("input column with ML attribute") {
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = sqlContext.createDataFrame(Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply)).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
    val encoder = new OneHotEncoder()
      .setInputCol("size")
      .setOutputCol("encoded")
    val output = encoder.transform(df)
    val group = AttributeGroup.fromStructField(output.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("size_is_small").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("size_is_medium").withIndex(1))
  }

  test("input column without ML attribute") {
    val df = sqlContext.createDataFrame(Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply)).toDF("index")
    val encoder = new OneHotEncoder()
      .setInputCol("index")
      .setOutputCol("encoded")
    val output = encoder.transform(df)
    val group = AttributeGroup.fromStructField(output.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("index_is_0").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("index_is_1").withIndex(1))
  }
}
