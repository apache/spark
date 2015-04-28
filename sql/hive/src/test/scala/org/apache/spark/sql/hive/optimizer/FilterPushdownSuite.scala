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

package org.apache.spark.sql.hive.optimizer

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.{QueryTest, _}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import org.apache.spark.sql.hive.test.TestHive._

case class TestData(a: Int, b: Int, c: Int, d: Int)

class FilterPushdownSuite extends QueryTest with BeforeAndAfter {
  import org.apache.spark.sql.hive.test.TestHive.implicits._


  val testData = TestHive.sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i + 1, i + 10, i + 20))).toDF()
  
  before {
    // Since every we are doing tests for DDL statements,
    // it is better to reset before every test.
    TestHive.reset()
    // Register the testData, which will be used in every test.
    testData.registerTempTable("testData")
  }

  test("Remove unnecessary attributes when resolving GroupingSets") {
    val sqlString = "SELECT a, SUM(c) FROM testData GROUP BY a, b GROUPING SETS ((a, b), a)"
    val queryExecution = sql(sqlString).queryExecution

    // Since the field `d` is not referred in Aggregate node, it will be removed from
    // the GroupExpressions in optimizedPlan
    val groupExpressions = queryExecution.optimizedPlan.collect {
      case e: Expand => e
    } match {
      case Seq(e: Expand) => e.projections // Expand.projections is Seq[GroupExpression]
      case _ => fail(s"More than one Expand found\n$queryExecution")
    }
    groupExpressions.foreach(_.collect {
      case ne: NamedExpression if ne.name == "d" =>
        fail(s"Attribute ${ne.name} should not be found after optimizaton")
    })
  }
}
