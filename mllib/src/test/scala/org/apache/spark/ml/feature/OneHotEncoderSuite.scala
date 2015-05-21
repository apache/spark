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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame


class OneHotEncoderSuite extends FunSuite with MLlibTestSparkContext {

  def stringIndexed(): DataFrame = {
    val data = sc.parallelize(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")), 2)
    val df = sqlContext.createDataFrame(data).toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    indexer.transform(df)
  }

  test("OneHotEncoder includeFirst = true") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val encoded = encoder.transform(transformed)

    val output = encoded.select("id", "labelVec").map { r =>
      val vec = r.get(1).asInstanceOf[Vector]
      (r.getInt(0), vec(0), vec(1), vec(2))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 1.0, 0.0, 0.0), (1, 0.0, 0.0, 1.0), (2, 0.0, 1.0, 0.0),
      (3, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 1.0, 0.0))
    assert(output === expected)
  }

  test("OneHotEncoder includeFirst = false") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setIncludeFirst(false)
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val encoded = encoder.transform(transformed)

    val output = encoded.select("id", "labelVec").map { r =>
      val vec = r.get(1).asInstanceOf[Vector]
      (r.getInt(0), vec(0), vec(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 0.0, 0.0), (1, 0.0, 1.0), (2, 1.0, 0.0),
      (3, 0.0, 0.0), (4, 0.0, 0.0), (5, 1.0, 0.0))
    assert(output === expected)
  }

}
