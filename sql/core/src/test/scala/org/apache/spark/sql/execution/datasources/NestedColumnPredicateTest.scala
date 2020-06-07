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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.StructType

/**
 * Defines common stuff for nested column predicate pushdown test, e.g. `ParquetFilterSuite`.
 */
trait NestedColumnPredicateTest {
  /**
   * Takes single level `inputDF` dataframe to generate multi-level nested
   * dataframes as new test data.
   *
   * This method accepts a function to run test. The given function takes three
   * parameters: a DataFrame which ranges from zero-nested to multi-level nested,
   * a string of the primitive field name, and a function that produces expected
   * result of collected column.
   */
  protected def withNestedDataFrame(inputDF: DataFrame)
      (runTest: (DataFrame, String, Any => Any) => Unit): Unit = {
    assert(inputDF.schema.fields.length == 1)
    assert(!inputDF.schema.fields.head.dataType.isInstanceOf[StructType])
    val df = inputDF.toDF("temp")
    Seq(
      (
        df.withColumnRenamed("temp", "a"),
        "a", // zero nesting
        (x: Any) => x),
      (
        df.withColumn("a", struct(df("temp") as "b")).drop("temp"),
        "a.b", // one level nesting
        (x: Any) => Row(x)),
      (
        df.withColumn("a", struct(struct(df("temp") as "c") as "b")).drop("temp"),
        "a.b.c", // two level nesting
        (x: Any) => Row(Row(x))
      ),
      (
        df.withColumnRenamed("temp", "a.b"),
        "`a.b`", // zero nesting with column name containing `dots`
        (x: Any) => x
      ),
      (
        df.withColumn("a.b", struct(df("temp") as "c.d") ).drop("temp"),
        "`a.b`.`c.d`", // one level nesting with column names containing `dots`
        (x: Any) => Row(x)
      )
    ).foreach { case (df, colName, resultFun) =>
      runTest(df, colName, resultFun)
    }
  }
}
