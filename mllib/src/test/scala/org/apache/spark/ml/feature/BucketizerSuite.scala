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

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class BucketizerSuite extends FunSuite with MLlibTestSparkContext {

  test("Bucket continuous features with setter") {
    val sqlContext = new SQLContext(sc)
    val data = Array(0.1, -0.5, 0.2, -0.3, 0.8, 0.7, -0.1, -0.4)
    val buckets = Array(-0.5, 0.0, 0.5)
    val bucketizedData = Array(2.0, 0.0, 2.0, 1.0, 3.0, 3.0, 1.0, 1.0)
    val dataFrame: DataFrame = sqlContext.createDataFrame(
        data.zip(bucketizedData)).toDF("feature", "expected")

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(buckets)

    bucketizer.transform(dataFrame).select("result", "expected").collect().foreach {
      case Row(x: Double, y: Double) =>
        assert(x === y, "The feature value is not correct after bucketing.")
    }
  }

  test("Binary search for finding buckets") {

  }
}
