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

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._

case class TestData(key: Int, value: String)

object TestData {
  val testData: SchemaRDD = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString)))
  testData.registerAsTable("testData")

  case class TestData2(a: Int, b: Int)
  val testData2: SchemaRDD =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
      TestData2(1, 2) ::
      TestData2(2, 1) ::
      TestData2(2, 2) ::
      TestData2(3, 1) ::
      TestData2(3, 2) :: Nil)
  testData2.registerAsTable("testData2")

  // TODO: There is no way to express null primitives as case classes currently...
  val testData3 =
    logical.LocalRelation('a.int, 'b.int).loadData(
      (1, null) ::
      (2, 2) :: Nil)

  case class UpperCaseData(N: Int, L: String)
  val upperCaseData =
    TestSQLContext.sparkContext.parallelize(
      UpperCaseData(1, "A") ::
      UpperCaseData(2, "B") ::
      UpperCaseData(3, "C") ::
      UpperCaseData(4, "D") ::
      UpperCaseData(5, "E") ::
      UpperCaseData(6, "F") :: Nil)
  upperCaseData.registerAsTable("upperCaseData")

  case class LowerCaseData(n: Int, l: String)
  val lowerCaseData =
    TestSQLContext.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
      LowerCaseData(2, "b") ::
      LowerCaseData(3, "c") ::
      LowerCaseData(4, "d") :: Nil)
  lowerCaseData.registerAsTable("lowerCaseData")

  case class ArrayData(data: Seq[Int], nestedData: Seq[Seq[Int]])
  val arrayData =
    TestSQLContext.sparkContext.parallelize(
      ArrayData(Seq(1,2,3), Seq(Seq(1,2,3))) ::
      ArrayData(Seq(2,3,4), Seq(Seq(2,3,4))) :: Nil)
  arrayData.registerAsTable("arrayData")
}
