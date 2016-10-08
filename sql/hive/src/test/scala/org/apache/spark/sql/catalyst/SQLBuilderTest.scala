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

package org.apache.spark.sql.catalyst

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.test.TestHiveSingleton


abstract class SQLBuilderTest extends QueryTest with TestHiveSingleton {
  protected def checkSQL(e: Expression, expectedSQL: String): Unit = {
    val actualSQL = e.sql
    try {
      assert(actualSQL === expectedSQL)
    } catch {
      case cause: Throwable =>
        fail(
          s"""Wrong SQL generated for the following expression:
             |
             |${e.prettyName}
             |
             |$cause
           """.stripMargin)
    }
  }

  protected def checkSQL(plan: LogicalPlan, expectedSQL: String): Unit = {
    val generatedSQL = try new SQLBuilder(plan).toSQL catch { case NonFatal(e) =>
      fail(
        s"""Cannot convert the following logical query plan to SQL:
           |
           |${plan.treeString}
         """.stripMargin)
    }

    try {
      assert(generatedSQL === expectedSQL)
    } catch {
      case cause: Throwable =>
        fail(
          s"""Wrong SQL generated for the following logical query plan:
             |
             |${plan.treeString}
             |
             |$cause
           """.stripMargin)
    }

    checkAnswer(spark.sql(generatedSQL), Dataset.ofRows(spark, plan))
  }

  protected def checkSQL(df: DataFrame, expectedSQL: String): Unit = {
    checkSQL(df.queryExecution.analyzed, expectedSQL)
  }
}
