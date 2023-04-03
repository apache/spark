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
package org.apache.spark.sql

import java.util.Arrays

import io.grpc.StatusRuntimeException

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.connect.client.util.RemoteSparkSession

/**
 * All tests in this class requires client UDF artifacts synced with the server. TODO: It means
 * these tests only works with SBT for now.
 */
class KeyValueGroupedDatasetE2ETestSuite extends RemoteSparkSession {
  // TODO: RowEncoder not supported?
//  test("flatGroupMap dataframe") {
//    val rows = Arrays.asList(Row("a", 10), Row("a", 20), Row("b", 1), Row("b", 2), Row("c", 1))
//    val schema = StructType(Array(
//      StructField("name", StringType), StructField("data", IntegerType)))
//    val df = spark.createDataFrame(rows, schema)
//    val values = df.groupByKey(_.getString(0))(StringEncoder)
//      .flatMapGroups((_, it) => Seq(it.toSeq.size))(PrimitiveIntEncoder).collectAsList()
//    assert(values == Arrays.asList[Int](2, 2, 1))
//  }

  test("flatGroupMap") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
      .flatMapGroups((_, it) => Seq(it.toSeq.size))(PrimitiveIntEncoder)
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("keys") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Long](0, 1))
  }

  test("keyAs - keys") {
    // Cannot up cast value e.g. It is okay to cast from Long to Double, but not Long to Int.
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
      .keyAs[Double](PrimitiveDoubleEncoder)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Double](0, 1))
  }

  test("keyAs - flatGroupMap") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
      .keyAs[Double](PrimitiveDoubleEncoder)
      .flatMapGroups((_, it) => Seq(it.toSeq.size))(PrimitiveIntEncoder)
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("mapValues - flatGroupMap") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
      .mapValues(v => v * 2)(PrimitiveLongEncoder)
      .flatMapGroups((_, it) => Seq(it.toSeq.sum))(PrimitiveLongEncoder)
      .collectAsList()
    assert(values == Arrays.asList[Long](40, 50))
  }

  test("mapValues - keys") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
      .mapValues(v => v * 2)(PrimitiveLongEncoder)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Long](0, 1))
  }

  test("flatMapSortedGroups") {
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)(PrimitiveLongEncoder)
    val aggregated = grouped
      .flatMapSortedGroups(functions.desc("id")) { (g, iter) =>
        Iterator(String.valueOf(g), iter.mkString(","))
      }(StringEncoder)
      .collectAsList()

    assert(aggregated == Arrays.asList[String]("0", "8,6,4,2,0", "1", "9,7,5,3,1"))

    // Star is not allowed as group sort column
    val message = intercept[StatusRuntimeException] {
      grouped
        .flatMapSortedGroups(functions.col("*")) { (g, iter) =>
          Iterator(String.valueOf(g), iter.mkString(","))
        }(StringEncoder)
        .collectAsList()
    }.getMessage
    assert(message.contains("Invalid usage of '*' in MapGroups"))
  }
}
