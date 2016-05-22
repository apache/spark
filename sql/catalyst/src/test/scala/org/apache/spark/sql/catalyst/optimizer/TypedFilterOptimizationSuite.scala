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

package org.apache.spark.sql.catalyst.optimizer

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.analysis.UnresolvedDeserializer
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, ReferenceToExpressions}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.BooleanType

class TypedFilterOptimizationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("EliminateSerialization", FixedPoint(50),
        EliminateSerialization) ::
      Batch("EmbedSerializerInFilter", FixedPoint(50),
        EmbedSerializerInFilter,
        RemoveAliasOnlyProject,
        CombineFilters) :: Nil
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()

  test("embed deserializer in filter condition if there is only one filter") {
    val input = LocalRelation('_1.int, '_2.int)
    val f = (i: (Int, Int)) => i._1 > 0

    val query = input.filter(f).analyze

    val optimized = Optimize.execute(query)

    val deserializer = input.deserialize[(Int, Int)].analyze
      .asInstanceOf[DeserializeToObject].deserializer
    val boundReference = BoundReference(0, deserializer.dataType, nullable = false)
    val callFunc = callFunction(f, BooleanType, boundReference)
    val condition = ReferenceToExpressions(callFunc, deserializer :: Nil)
    val expected = input.where(condition).analyze

    comparePlans(optimized, expected)
  }

  test("embed deserializer in filter condition if there are two filters") {
    val input = LocalRelation('_1.int, '_2.int)
    val f1 = (i: (Int, Int)) => i._1 > 0
    val f2 = (i: (Int, Int)) => i._2 > 0

    val query = input.filter(f1).filter(f2).analyze

    val optimized = Optimize.execute(query)

    val deserializer = input.deserialize[(Int, Int)].analyze
      .asInstanceOf[DeserializeToObject].deserializer
    val boundReference = BoundReference(0, deserializer.dataType, nullable = false)
    val callF1 = callFunction(f1, BooleanType, boundReference)
    val callF2 = callFunction(f2, BooleanType, boundReference)
    val condition = ReferenceToExpressions(callF2 && callF1, deserializer :: Nil)
    val expected = input.where(condition).analyze

    comparePlans(optimized, expected)
  }
}
