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
package org.apache.spark.sql.connect.planner

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * [[SparkConnectPlanTestWithSparkSession]] contains a SparkSession for the connect planner.
 *
 * It is not recommended to use Catalyst DSL along with this trait because `SharedSparkSession`
 * has also defined implicits over Catalyst LogicalPlan which will cause ambiguity with the
 * implicits defined in Catalyst DSL.
 */
trait SparkConnectPlanTestWithSparkSession extends SharedSparkSession with SparkConnectPlanTest {
  override def getSession(): SparkSession = spark
}

class SparkConnectDeduplicateSuite extends SparkConnectPlanTestWithSparkSession {
  lazy val connectTestRelation = createLocalRelationProto(
    Seq(
      AttributeReference("id", IntegerType)(),
      AttributeReference("key", StringType)(),
      AttributeReference("value", StringType)()))

  lazy val sparkTestRelation = {
    spark.createDataFrame(
      new java.util.ArrayList[Row](),
      StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("key", StringType),
          StructField("value", StringType))))
  }

  test("Test basic deduplicate") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.plans._
      Dataset.ofRows(spark, transform(connectTestRelation.distinct()))
    }

    val sparkPlan = sparkTestRelation.distinct()
    comparePlans(connectPlan.queryExecution.analyzed, sparkPlan.queryExecution.analyzed, false)

    val connectPlan2 = {
      import org.apache.spark.sql.connect.dsl.plans._
      Dataset.ofRows(spark, transform(connectTestRelation.deduplicate(Seq("key", "value"))))
    }
    val sparkPlan2 = sparkTestRelation.dropDuplicates(Seq("key", "value"))
    comparePlans(connectPlan2.queryExecution.analyzed, sparkPlan2.queryExecution.analyzed, false)
  }
}
