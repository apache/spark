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

import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{SQLConf, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.util.Random

class UnsafeExternalSortSuite extends SparkPlanTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    TestSQLContext.conf.setConf(SQLConf.CODEGEN_ENABLED, true)
  }

  override def afterAll(): Unit = {
    TestSQLContext.conf.setConf(SQLConf.CODEGEN_ENABLED, SQLConf.CODEGEN_ENABLED.defaultValue.get)
  }

  private def createRow(values: Any*): Row = {
    new GenericRow(values.map(CatalystTypeConverters.convertToCatalyst).toArray)
  }

  test("basic sorting") {

    val inputData = Seq(
      ("Hello", 9),
      ("World", 4),
      ("Hello", 7),
      ("Skinny", 0),
      ("Constantinople", 9)
    )

    val sortOrder: Seq[SortOrder] = Seq(
      SortOrder(BoundReference(0, StringType, nullable = false), Ascending),
      SortOrder(BoundReference(1, IntegerType, nullable = false), Descending))

    checkAnswer(
      Random.shuffle(inputData),
      (input: SparkPlan) => new UnsafeExternalSort(sortOrder, global = false, input),
      inputData
    )
  }
}
