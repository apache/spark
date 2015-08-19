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
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * An end-to-end test suite specifically for testing Tungsten (Unsafe/CodeGen) mode.
 *
 * This is here for now so I can make sure Tungsten project is tested without refactoring existing
 * end-to-end test infra. In the long run this should just go away.
 */
class DataFrameTungstenSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("test simple types") {
    withSQLConf(SQLConf.UNSAFE_ENABLED.key -> "true") {
      val df = sqlContext.sparkContext.parallelize(Seq((1, 2))).toDF("a", "b")
      assert(df.select(struct("a", "b")).first().getStruct(0) === Row(1, 2))
    }
  }

  test("test struct type") {
    withSQLConf(SQLConf.UNSAFE_ENABLED.key -> "true") {
      val struct = Row(1, 2L, 3.0F, 3.0)
      val data = sqlContext.sparkContext.parallelize(Seq(Row(1, struct)))

      val schema = new StructType()
        .add("a", IntegerType)
        .add("b",
          new StructType()
            .add("b1", IntegerType)
            .add("b2", LongType)
            .add("b3", FloatType)
            .add("b4", DoubleType))

      val df = sqlContext.createDataFrame(data, schema)
      assert(df.select("b").first() === Row(struct))
    }
  }

  test("test nested struct type") {
    withSQLConf(SQLConf.UNSAFE_ENABLED.key -> "true") {
      val innerStruct = Row(1, "abcd")
      val outerStruct = Row(1, 2L, 3.0F, 3.0, innerStruct, "efg")
      val data = sqlContext.sparkContext.parallelize(Seq(Row(1, outerStruct)))

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

      val df = sqlContext.createDataFrame(data, schema)
      assert(df.select("b").first() === Row(outerStruct))
    }
  }
}
