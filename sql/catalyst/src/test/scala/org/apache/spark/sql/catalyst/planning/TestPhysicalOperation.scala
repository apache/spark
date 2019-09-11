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

package org.apache.spark.sql.catalyst.planning

import org.junit.Assert

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId, IsNotNull}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}

class TestPhysicalOperation extends SparkFunSuite {
  private val textField = AttributeReference("text1", StringType, nullable = false)()
  private val dateField = AttributeReference("date1", TimestampType)()
  private val longField = AttributeReference("long1", LongType)()
  private val longAliasField = Alias(longField, "long1")(exprId = ExprId(100))
  private val longAliasField2 =
    Alias(longField, "long1")(exprId = ExprId(100), qualifier = Seq("tmp"))
  private val testRelation = LocalRelation(textField, dateField, longField)

  object InvokePhysicalOperation {
    def apply(plan: LogicalPlan): Seq[Expression] = plan match {
      case PhysicalOperation(projects, filters, _) =>
        filters
      case _ => Nil
    }
  }

  test("verify PhysicalOperation returns correct filters with substituted alias when" +
    " AttributeReferences are same in alias map and filter condition") {
    val actualFilters = InvokePhysicalOperation(
      Filter(IsNotNull(longAliasField.toAttribute),
        Project(Seq(longAliasField, textField),
          Filter(IsNotNull(dateField),
            testRelation))))
    val expectedFilters = Seq(IsNotNull(dateField), IsNotNull(longAliasField))

    Assert.assertEquals(expectedFilters, actualFilters)
  }

  test("SPARK-29029 verify PhysicalOperation returns correct filters with substituted alias when" +
    " AttributeReference's ExprId is same but qualifier is different") {
    val actualFilters = InvokePhysicalOperation(
      Filter(IsNotNull(longAliasField2.toAttribute),
        Project(Seq(longAliasField, textField),
          Filter(IsNotNull(dateField),
            testRelation))))
    val expectedFilters = Seq(IsNotNull(dateField), IsNotNull(longAliasField2))

    Assert.assertEquals(expectedFilters, actualFilters)
  }
}
