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

import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.expressions.{Ascending, BoundReference, AttributeReference, SortOrder}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{Row, SQLConf}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.TestSQLContext

import scala.util.Random

class UnsafeExternalSortSuite extends SparkPlanTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    TestSQLContext.conf.setConf(SQLConf.CODEGEN_ENABLED, true)
  }

  override def afterAll(): Unit = {
    TestSQLContext.conf.setConf(SQLConf.CODEGEN_ENABLED, SQLConf.CODEGEN_ENABLED.defaultValue.get)
  }

  test("basic sorting") {
    val input = Seq(
      ("Hello", 9, 1.0),
      ("World", 4, 2.0),
      ("Hello", 7, 8.1),
      ("Skinny", 0, 2.2),
      ("Constantinople", 9, 1.1)
    )

    checkAnswer(
      Random.shuffle(input).toDF("a", "b", "c"),
      ExternalSort('a.asc :: 'b.asc :: Nil, global = false, _: SparkPlan),
      input.sorted)

    checkAnswer(
      Random.shuffle(input).toDF("a", "b", "c"),
      ExternalSort('b.asc :: 'a.asc :: Nil, global = false, _: SparkPlan),
      input.sortBy(t => (t._2, t._1)))
  }

  test("sorting with object columns") {
    // TODO: larger input data
    val input = Seq(
      Row("Hello", Row(1)),
      Row("World", Row(2))
    )

    val schema = StructType(
      StructField("a", StringType, nullable = false) ::
      StructField("b", StructType(StructField("b", IntegerType, nullable = false) :: Nil)) ::
      Nil
    )

    // Hack so that we don't need to pass in / mock TaskContext, SparkEnv, etc. Ultimately it would
    // be better to not use this hack, but due to time constraints I have deferred this for
    // followup PRs.
    val sortResult = TestSQLContext.sparkContext.parallelize(input, 1).mapPartitions { iter =>
      val rows = iter.toSeq
      val sortOrder = SortOrder(BoundReference(0, StringType, nullable = false), Ascending)

      val sorter = new UnsafeExternalRowSorter(
        schema,
        GenerateOrdering.generate(Seq(sortOrder), schema.toAttributes),
        new PrefixComparator {
          override def compare(prefix1: Long, prefix2: Long): Int = 0
        },
        x => 0L
      )

      val toCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)

      sorter.insertRow(toCatalyst(input.head).asInstanceOf[InternalRow])
      sorter.spill()
      input.tail.foreach { row =>
        sorter.insertRow(toCatalyst(row).asInstanceOf[InternalRow])
      }
      val sortedRowsIterator = sorter.sort()
      sortedRowsIterator.map(CatalystTypeConverters.convertToScala(_, schema).asInstanceOf[Row])
    }.collect()

    assert(input.sortBy(t => t.getString(0)) === sortResult)
  }
}
