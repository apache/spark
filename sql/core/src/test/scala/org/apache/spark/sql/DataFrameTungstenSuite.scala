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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * An end-to-end test suite specifically for testing Tungsten (Unsafe/CodeGen) mode.
 *
 * This is here for now so I can make sure Tungsten project is tested without refactoring existing
 * end-to-end test infra. In the long run this should just go away.
 */
class DataFrameTungstenSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("test simple types") {
    val df = sparkContext.parallelize(Seq((1, 2))).toDF("a", "b")
    assert(df.select(struct("a", "b")).first().getStruct(0) === Row(1, 2))
  }

  test("test struct type") {
    val struct = Row(1, 2L, 3.0F, 3.0)
    val data = sparkContext.parallelize(Seq(Row(1, struct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType))

    val df = spark.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(struct))
  }

  test("test nested struct type") {
    val innerStruct = Row(1, "abcd")
    val outerStruct = Row(1, 2L, 3.0F, 3.0, innerStruct, "efg")
    val data = sparkContext.parallelize(Seq(Row(1, outerStruct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType)
          .add("b5", new StructType()
          .add("b5a", IntegerType)
          .add("b5b", StringType))
          .add("b6", StringType))

    val df = spark.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(outerStruct))
  }

  test("primitive data type accesses in persist data") {
    val data = Seq(true, 1.toByte, 3.toShort, 7, 15.toLong,
      31.25.toFloat, 63.75, null)
    val dataTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, IntegerType)
    val schemas = dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, true)
    }
    val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(data)))
    val df = spark.createDataFrame(rdd, StructType(schemas))
    val row = df.persist.take(1).apply(0)
    checkAnswer(df, row)
  }

  test("access cache multiple times") {
    val df0 = sparkContext.parallelize(Seq(1, 2, 3), 1).toDF("x").cache
    df0.count
    val df1 = df0.filter("x > 1")
    checkAnswer(df1, Seq(Row(2), Row(3)))
    val df2 = df0.filter("x > 2")
    checkAnswer(df2, Row(3))

    val df10 = sparkContext.parallelize(Seq(3, 4, 5, 6), 1).toDF("x").cache
    for (_ <- 0 to 2) {
      val df11 = df10.filter("x > 5")
      checkAnswer(df11, Row(6))
    }
  }

  test("access only some column of the all of columns") {
    val df = spark.range(1, 10).map(i => (i, (i + 1).toDouble)).toDF("l", "d")
    df.cache
    df.count
    assert(df.filter("d < 3").count == 1)
  }
}
