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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.sql.{SQLConf, SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._

class UnsafeExternalSortSuite extends FunSuite with Matchers {

  private def createRow(values: Any*): Row = {
    new GenericRow(values.map(CatalystTypeConverters.convertToCatalyst).toArray)
  }

  test("basic sorting") {
    val sc = TestSQLContext.sparkContext
    val sqlContext = new SQLContext(sc)
    sqlContext.conf.setConf(SQLConf.CODEGEN_ENABLED, "true")

    val schema: StructType = StructType(
      StructField("word", StringType, nullable = false) ::
      StructField("number", IntegerType, nullable = false) :: Nil)
    val sortOrder: Seq[SortOrder] = Seq(
      SortOrder(BoundReference(0, StringType, nullable = false), Ascending),
      SortOrder(BoundReference(1, IntegerType, nullable = false), Descending))
    val rowsToSort: Seq[Row] = Seq(
      createRow("Hello", 9),
      createRow("World", 4),
      createRow("Hello", 7),
      createRow("Skinny", 0),
      createRow("Constantinople", 9))
    SparkPlan.currentContext.set(sqlContext)
    val input =
      new PhysicalRDD(schema.toAttributes.map(_.toAttribute), sc.parallelize(rowsToSort, 1))
    // Treat the existing sort operators as the source-of-truth for this test
    val defaultSorted = new Sort(sortOrder, global = false, input).executeCollect()
    val externalSorted = new ExternalSort(sortOrder, global = false, input).executeCollect()
    val unsafeSorted = new UnsafeExternalSort(sortOrder, global = false, input).executeCollect()
    assert (defaultSorted === externalSorted)
    assert (unsafeSorted === externalSorted)
  }
}
