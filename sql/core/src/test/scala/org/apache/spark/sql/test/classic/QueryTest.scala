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

package org.apache.spark.sql.test.classic

import scala.language.implicitConversions

import org.apache.spark.sql.{classic, QueryTest => BaseQueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.FilterExec

/**
 * Extends [[BaseQueryTest]] to explicitly provide [[classic.SparkSession]] etc.
 *
 * Use this trait and [[org.apache.spark.sql.test.SharedClassicSparkSession]] to indicate that
 * this test is classic-only, i.e. it is not intended to run this test with a
 * [[org.apache.spark.sql.connect.SparkSession]].
 */
trait QueryTest extends BaseQueryTest with classic.SparkSessionProvider {

  override protected lazy val sql: String => classic.DataFrame = spark.sql _

  protected object classicTestImplicits
    extends classic.SQLImplicits
      with classic.ClassicConversions
      with classic.ColumnConversions {
    override protected def session: classic.SparkSession = spark
    override protected def converter: classic.ColumnNodeToExpressionConverter = spark.converter
  }

  /**
   * Strip Spark-side filtering in order to check if a datasource filters rows correctly.
   */
  protected def stripSparkFilter(df: classic.DataFrame): classic.DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case FilterExec(_, child) => child
    }

    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }

  /**
   * Turn a logical plan into a `DataFrame`. This should be removed once we have an easier
   * way to construct `DataFrame` directly out of local data without relying on implicits.
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): classic.DataFrame = {
    classic.Dataset.ofRows(spark, plan)
  }
}
