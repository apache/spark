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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

class ExistingRDDSuite extends PlanTest {
  private val nestedTypes = Seq(
    ArrayType(IntegerType, containsNull = false),
    StructType(Seq(StructField("x", IntegerType, nullable = false))),
    MapType(IntegerType, ArrayType(IntegerType, containsNull = false), valueContainsNull = false))

  // Recursive references can widen nested nullability and rename the previous iteration's output.
  test("SPARK-60860: retain statistics and constraints with compatible nested nullability") {
    nestedTypes.foreach { dataType =>
      val value = AttributeReference("value", dataType, nullable = false)()
      val number = AttributeReference("number", IntegerType, nullable = false)()
      val source = Filter(GreaterThan(number, Literal(5)), LocalRelation(value, number))
      val output = source.output.map { attr =>
        attr.newInstance().withName(s"recursive_${attr.name}")
          .withDataType(attr.dataType.asNullable).withNullability(true)
      }
      val ref = UnionLoopRef(0, output, accumulated = false)
      val (stats, constraints) =
        LogicalRDD.rewriteStatsAndConstraints(
          ref, source, sameOutput = false, allowCompatibleNullability = true)
      assert(stats.contains(source.stats))
      val mapping = source.output.zip(output).toMap
      assert(constraints.contains(LogicalRDD.rewriteConstraints(source.constraints, mapping)))
      assert(constraints.get.exists(_.references.contains(output(1))))
      assert(LogicalRDD.buildOutputAssocForRewrite(source.output,
        output.zip(source.output).map { case (dest, src) => dest.withName(src.name) },
        allowCompatibleNullability = true).isDefined)
      assert(LogicalRDD.buildOutputAssocForRewrite(
        source.output, output, allowCompatibleNullability = true).isEmpty)
    }
  }

  // Checkpointing and other callers retain exact nested type matching unless they opt in.
  test("SPARK-60860: compatible nested nullability requires explicit opt-in") {
    nestedTypes.foreach { dataType =>
      val source = AttributeReference("value", dataType, nullable = false)()
      val sameType = source.newInstance().withNullability(true)
      val widened = sameType.withDataType(dataType.asNullable)
      assert(LogicalRDD.buildOutputAssocForRewrite(Seq(source), Seq(sameType)).isDefined)
      assert(LogicalRDD.buildOutputAssocForRewrite(Seq(source), Seq(widened)).isEmpty)
      assert(LogicalRDD.buildOutputAssocForRewriteWithNewOutput(
        Seq(source), Seq(widened)).isEmpty)
      val (stats, constraints) = LogicalRDD.rewriteStatsAndConstraints(
        LocalRelation(widened), LocalRelation(source))
      assert(stats.isEmpty)
      assert(constraints.isEmpty)
    }
  }

  // A nullable source cannot be rewritten as a destination that forbids nested nulls.
  test("SPARK-60860: reject narrowing nested nullability") {
    for (dataType <- nestedTypes; allowCompatibleNullability <- Seq(false, true)) {
      val source = AttributeReference("value", dataType.asNullable)()
      val destination = AttributeReference("value", dataType)()
      assert(LogicalRDD.buildOutputAssocForRewrite(
        Seq(source), Seq(destination), allowCompatibleNullability).isEmpty)
      assert(LogicalRDD.buildOutputAssocForRewriteWithNewOutput(
        Seq(source), Seq(destination.withName("renamed")), allowCompatibleNullability).isEmpty)
    }
  }

  // Type compatibility must still preserve element types and nested field names.
  test("SPARK-60860: reject incompatible output types") {
    val source = AttributeReference("value", ArrayType(IntegerType, containsNull = false))()
    val destination = AttributeReference("value", ArrayType(LongType, containsNull = true))()
    val struct = AttributeReference("value", nestedTypes(1))()
    val renamed = AttributeReference("value", new StructType().add("y", IntegerType))()
    Seq(false, true).foreach { allowCompatibleNullability =>
      assert(LogicalRDD.buildOutputAssocForRewrite(
        Seq(source), Seq(destination), allowCompatibleNullability).isEmpty)
      assert(LogicalRDD.buildOutputAssocForRewriteWithNewOutput(
        Seq(source), Seq(destination), allowCompatibleNullability).isEmpty)
      assert(LogicalRDD.buildOutputAssocForRewriteWithNewOutput(
        Seq(struct), Seq(renamed), allowCompatibleNullability).isEmpty)
    }
  }
}
