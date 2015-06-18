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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.SQLConf
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
}
