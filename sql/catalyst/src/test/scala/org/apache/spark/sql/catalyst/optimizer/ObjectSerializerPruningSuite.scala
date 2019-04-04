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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ObjectSerializerPruningSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Object serializer pruning", FixedPoint(100),
      ObjectSerializerPruning,
      RemoveNoopOperators) :: Nil
  }

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()

  test("collect struct types") {
    val dataTypes = Seq(
      IntegerType,
      ArrayType(IntegerType),
      StructType.fromDDL("a int, b int"),
      ArrayType(StructType.fromDDL("a int, b int, c string")),
      StructType.fromDDL("a struct<a:int, b:int>, b int"),
      MapType(IntegerType, StructType.fromDDL("a int, b int, c string")),
      MapType(StructType.fromDDL("a struct<a:int, b:int>, b int"), IntegerType),
      MapType(StructType.fromDDL("a int, b int"), StructType.fromDDL("c long, d string"))
    )

    val expectedTypes = Seq(
      Seq.empty[StructType],
      Seq.empty[StructType],
      Seq(StructType.fromDDL("a int, b int")),
      Seq(StructType.fromDDL("a int, b int, c string")),
      Seq(StructType.fromDDL("a struct<a:int, b:int>, b int"),
        StructType.fromDDL("a int, b int")),
      Seq(StructType.fromDDL("a int, b int, c string")),
      Seq(StructType.fromDDL("a struct<a:int, b:int>, b int"),
        StructType.fromDDL("a int, b int")),
      Seq(StructType.fromDDL("a int, b int"), StructType.fromDDL("c long, d string"))
    )

    dataTypes.zipWithIndex.foreach { case (dt, idx) =>
      val structs = ObjectSerializerPruning.collectStructType(dt, ArrayBuffer.empty[StructType])
      assert(structs === expectedTypes(idx))
    }
  }

  test("SPARK-26619: Prune the unused serializers from SerializeFromObject") {
    val testRelation = LocalRelation('_1.int, '_2.int)
    val serializerObject = CatalystSerde.serialize[(Int, Int)](
      CatalystSerde.deserialize[(Int, Int)](testRelation))
    val query = serializerObject.select('_1)
    val optimized = Optimize.execute(query.analyze)
    val expected = serializerObject.copy(serializer = Seq(serializerObject.serializer.head)).analyze
    comparePlans(optimized, expected)
  }

  test("Prune nested serializers") {
    withSQLConf(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      val testRelation = LocalRelation('_1.struct(StructType.fromDDL("_1 int, _2 string")), '_2.int)
      val serializerObject = CatalystSerde.serialize[((Int, String), Int)](
        CatalystSerde.deserialize[((Int, String), Int)](testRelation))
      val query = serializerObject.select($"_1._1")
      val optimized = Optimize.execute(query.analyze)

      val prunedSerializer = serializerObject.serializer.head.transformDown {
        case CreateNamedStruct(children) =>
          CreateNamedStruct(children.take(2))
      }.transformUp {
        // Aligns null literal in `If` expression to make it resolvable.
        case i @ If(_: IsNull, Literal(null, dt), ser) if !dt.sameType(ser.dataType) =>
          i.copy(trueValue = Literal(null, ser.dataType))
      }.asInstanceOf[NamedExpression]

      // `name` in `GetStructField` affects `comparePlans`. Maybe we can ignore
      // `name` in `GetStructField.equals`?
      val expected = serializerObject.copy(serializer = Seq(prunedSerializer))
        .select($"_1._1").analyze.transformAllExpressions {
        case g: GetStructField => g.copy(name = None)
      }
      comparePlans(optimized, expected)
    }
  }
}
