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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

class EliminateMapObjectsSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = {
      Batch("EliminateMapObjects", FixedPoint(50),
        NullPropagation,
        SimplifyCasts,
        EliminateMapObjects) :: Nil
    }
  }

  implicit private def intArrayEncoder = ExpressionEncoder[Array[Int]]()
  implicit private def doubleArrayEncoder = ExpressionEncoder[Array[Double]]()

  test("SPARK-20254: Remove unnecessary data conversion for primitive array") {
    val intObjType = ObjectType(classOf[Array[Int]])
    val intInput = LocalRelation('a.array(ArrayType(IntegerType, false)))
    val intQuery = intInput.deserialize[Array[Int]].analyze
    val intOptimized = Optimize.execute(intQuery)
    val intExpected = DeserializeToObject(
      Invoke(intInput.output(0), "toIntArray", intObjType, Nil, true, false),
      AttributeReference("obj", intObjType, true)(), intInput)
    comparePlans(intOptimized, intExpected)

    val doubleObjType = ObjectType(classOf[Array[Double]])
    val doubleInput = LocalRelation('a.array(ArrayType(DoubleType, false)))
    val doubleQuery = doubleInput.deserialize[Array[Double]].analyze
    val doubleOptimized = Optimize.execute(doubleQuery)
    val doubleExpected = DeserializeToObject(
      Invoke(doubleInput.output(0), "toDoubleArray", doubleObjType, Nil, true, false),
      AttributeReference("obj", doubleObjType, true)(), doubleInput)
    comparePlans(doubleOptimized, doubleExpected)
  }
}
