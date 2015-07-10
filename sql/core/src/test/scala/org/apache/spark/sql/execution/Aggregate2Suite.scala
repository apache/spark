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

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, QueryTest}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.BeforeAndAfterAll

class Aggregate2Suite extends QueryTest with BeforeAndAfterAll {

  protected lazy val ctx = TestSQLContext

  var originalUseAggregate2: Boolean = _

  override def beforeAll(): Unit = {
    originalUseAggregate2 = ctx.conf.useSqlAggregate2
    ctx.sql("set spark.sql.useAggregate2=true")
    val data =
      Row(1, 10) ::
      Row(1, 20) ::
      Row(1, 30) ::
      Row(2, 0) ::
      Row(2, -1) ::
      Row(2, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(3, null) :: Nil
    val schema =
      StructType(StructField("key", IntegerType) :: StructField("value", IntegerType) :: Nil)
    ctx.createDataFrame(ctx.sparkContext.parallelize(data, 2), schema).registerTempTable("agg2")
  }

  test("test average2") {
    ctx.sql(
      """
        |SELECT key, avg2(value)
        |FROM agg2
        |GROUP BY key
      """.stripMargin).explain(true)

    ctx.sql(
      """
        |SELECT key, avg2(value)
        |FROM agg2
        |GROUP BY key
      """.stripMargin).queryExecution.executedPlan(3).execute().collect().foreach(println)

    checkAnswer(
      ctx.sql(
        """
          |SELECT key, avg2(value)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(1, 20.0) :: Row(2, -0.5) :: Row(3, null) :: Nil)

  }

  override def afterAll(): Unit = {
    ctx.sql(s"set spark.sql.useAggregate2=$originalUseAggregate2")
  }
}
