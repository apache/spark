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

import org.scalatest.Matchers

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

/**
* Created by eyalf on 11/4/2016.
* SPARK-18601 discusses simplification direct access to complex types creators.
* i.e. {{{create_named_struct(square, `x` * `x`).square}}} can be simplified to {{{`x` * `x`}}}.
* sam applies to create_array and create_map
*/
class ComplexTypesSuite extends PlanTest with Matchers{

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("collapse projections", FixedPoint(10),
          CollapseProject) ::
      Batch("Constant Folding", FixedPoint(10),
          NullPropagation(conf),
          ConstantFolding,
          BooleanSimplification,
          SimplifyConditionals,
          SimplifyCreateStructOps,
          SimplifyCreateArrayOps,
          SimplifyCreateMapOps) :: Nil
  }

  val idAtt = ('id).long.notNull

  lazy val baseOptimizedPlan = Range(1L, 1000L, 1, Some(2), idAtt :: Nil)

  val idRef = baseOptimizedPlan.output.head

  implicit class ComplexTypeDslSupport(e : Expression) {
    def getStructField(f : String): GetStructField = {
      e should be ('resolved)
      e.dataType should be (a[StructType])
      val structType = e.dataType.asInstanceOf[StructType]
      val ord = structType.fieldNames.indexOf(f)
      ord shouldNot be (-1)
      GetStructField(e, ord, Some(f))
    }

    def getArrayStructField(f : String) : Expression = {
      e should be ('resolved)
      e.dataType should be (a[ArrayType])
      val arrType = e.dataType.asInstanceOf[ArrayType]
      arrType.elementType should be (a[StructType])
      val structType = arrType.elementType.asInstanceOf[StructType]
      val ord = structType.fieldNames.indexOf(f)
      ord shouldNot be (-1)
      GetArrayStructFields(e, structType(ord), ord, 1, arrType.containsNull)
    }

    def getArrayItem(i : Int) : GetArrayItem = {
      e should be ('resolved)
      e.dataType should be (a[ArrayType])
      GetArrayItem(e, Literal(i))
    }

    def getMapValue(k : Expression) : Expression = {
      e should be ('resolved)
      e.dataType should be (a[MapType])
      val mapType = e.dataType.asInstanceOf[MapType]
      k.dataType shouldEqual mapType.keyType
      GetMapValue(e, k)
    }
  }

  test("explicit") {
    val rel = baseOptimizedPlan.select(
      CreateNamedStruct("att" :: idRef :: Nil).getStructField("att") as "outerAtt"
   )

    rel.schema shouldEqual
      StructType(StructField("outerAtt", LongType, nullable = false) :: Nil)

    val optimized = Optimize execute rel

    val expected = baseOptimizedPlan.select(idRef as "outerAtt")

    comparePlans(optimized, expected)
  }

  ignore("explicit - deduced att name") {
    val rel = baseOptimizedPlan.select(
      CreateNamedStruct("att" :: idRef :: Nil).getStructField("att")
   )
    rel.schema shouldEqual
      StructType(
        StructField("named_struct(att, id AS `att`).att", LongType, nullable = false) :: Nil
     )

    val optimized = Optimize execute rel

    val expected = baseOptimizedPlan.select(idRef as "named_struct(att, id AS `att`).att")

    comparePlans(optimized, expected)
  }

  test("collapsed") {
    val rel = baseOptimizedPlan.select(
      CreateNamedStruct("att" :: idRef :: Nil) as "struct1"
   )
    rel.schema shouldEqual
      StructType(
        StructField(
          "struct1",
          StructType(StructField("att", LongType, false) :: Nil),
          false
       ) :: Nil
     )

    val struct1Ref = rel.output.head
    val rel2 = rel.select(struct1Ref.getStructField("att").as("struct1Att"))

    rel2.schema shouldEqual
      StructType(
        StructField("struct1Att", LongType, false) :: Nil
     )

    val optimized = Optimize execute rel2
    val expected =
      baseOptimizedPlan.select(idRef as "struct1Att" )

    comparePlans(optimized, expected)
  }

  test("collapsed2") {
    val rel = baseOptimizedPlan.select(
      CreateNamedStruct(
        Literal("att1") :: idRef ::
        Literal("att2") :: (idRef * idRef) ::
        Nil) as "struct1"
   )
    rel.schema shouldEqual
      StructType(
        StructField(
          "struct1",
          StructType(
            StructField("att1", LongType, false) ::
            StructField("att2", LongType, false) :: Nil
         ),
          false
       ) :: Nil
     )

    val structRef = rel.output.head

    val rel2 = rel.select(
      structRef.getStructField("att1").as("struct1Att1"),
      structRef.getStructField("att2").as("struct1Att2"))

    rel2.schema shouldEqual
      StructType(
        StructField("struct1Att1", LongType, false) ::
        StructField("struct1Att2", LongType, false) ::
        Nil
     )

    val optimized = Optimize execute rel2
    val expected =
      baseOptimizedPlan.select(
        idRef as "struct1Att1",
        (idRef * idRef) as "struct1Att2"
     )

    comparePlans(optimized, expected)
  }

  ignore("collapsed2 - deduced names") {
    val rel = baseOptimizedPlan.select(
      CreateNamedStruct(
        Literal("att1") :: idRef ::
        Literal("att2") :: (idRef * idRef) ::
        Nil
     ) as "struct1"
   )
    rel.schema shouldEqual
      StructType(
        StructField(
          "struct1",
          StructType(
            StructField("att1", LongType, false) ::
            StructField("att2", LongType, false) :: Nil
         ),
          false
       ) :: Nil
     )
    val structRef = rel.output.head
    val rel2 = rel.select(
      structRef.getStructField("att1"),
      structRef.getStructField("att2"))

    rel2.schema shouldEqual
      StructType(
        StructField("struct1.att1", LongType, false) ::
        StructField("struct1.att2", LongType, false) ::
        Nil
     )

    val optimized = Optimize execute rel2
    val expected =
      baseOptimizedPlan.select(
        idRef as "struct1.att1",
        (idRef * idRef) as "struct1.att2"
     )

    comparePlans(optimized, expected)
  }

  test("simplified array ops") {
    val rel = baseOptimizedPlan.select(
      CreateArray(
        CreateNamedStruct(
          Literal("att1") :: idRef ::
          Literal("att2") :: (idRef * idRef) ::
          Nil
       ) ::
        CreateNamedStruct(
          Literal("att1") :: (idRef + 1L) ::
            Literal("att2") :: ((idRef + 1L) * (idRef + 1L)) ::
            Nil
       ) ::
        Nil
     ) as "arr"
   )
    rel.schema shouldEqual
      StructType(
        StructField(
          "arr",
          ArrayType(
            StructType(
              StructField("att1", LongType, false) ::
              StructField("att2", LongType, false) ::
              Nil
           ),
            false
         ),
          nullable = false
       ) :: Nil
     )

    val arrRef = rel.output.head
    val rel2 = rel.select(
      arrRef.getArrayStructField("att1") as "a1",
      arrRef.getArrayItem(1) as "a2",
      arrRef.getArrayItem(1).getStructField("att1") as "a3",
      arrRef.getArrayStructField("att1").getArrayItem(1) as "a4"
   )

    rel2.schema shouldEqual
      StructType(
        StructField("a1", ArrayType(LongType, false), nullable = false) ::
          StructField("a2",
            StructType(
              StructField("att1", LongType, nullable = false) ::
              StructField("att2", LongType, nullable = false) ::
              Nil
           ),
            nullable = true
         ) ::
          StructField("a3", LongType, nullable = true) ::
          StructField("a4", LongType, nullable = true) ::
          Nil
     )

    val optimized = Optimize execute rel2
    val expected =
      baseOptimizedPlan.select(
        CreateArray(idRef :: idRef + 1L :: Nil) as "a1",
        CreateNamedStruct(
          "att1" :: (idRef + 1L) ::
          Literal("att2") :: ((idRef + 1L) * (idRef + 1L)) ::
          Nil
       ) as "a2",
        (idRef + 1L) as "a3",
        (idRef + 1L) as "a4"
     )
    comparePlans(optimized, expected)
  }

  test("simplify map ops") {
    val rel = baseOptimizedPlan.select(
      CreateMap(
        Literal("r1") ::
        CreateNamedStruct(Literal("att1") :: idRef :: Nil) ::
        Literal("r2") ::
        CreateNamedStruct(Literal("att1") :: (idRef + 1L) :: Nil)
        :: Nil
     ) as "m"
   )
    rel.schema shouldEqual
      StructType(
        StructField(
          "m",
          MapType(
            StringType,
            StructType(StructField ("att1", LongType, nullable = false) :: Nil),
            valueContainsNull = false
         ),
          nullable = false
       )
        :: Nil
     )

    val mapRef = rel.output.head

    val rel2 = rel.select(
      mapRef.getMapValue("r1") as "a1",
      mapRef.getMapValue("r1").getStructField("att1") as "a2",
      mapRef.getMapValue("r32") as "a3",
      mapRef.getMapValue("r32").getStructField("att1") as "a4"
   )
    val optimized = Optimize execute rel2

    val expected =
      baseOptimizedPlan.select(
        CreateNamedStruct("att1" :: idRef:: Nil) as "a1",
        idRef as "a2",
        Literal.create(
          null,
          StructType(
            StructField("att1", LongType, nullable = false) :: Nil
         )
       ) as "a3",
        Literal.create(null, LongType) as "a4"
     )
    comparePlans(optimized, expected)
  }

  test("simplify map ops, constant lookup, dynamic keys") {
    val rel = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
        (idRef + 1L) :: (idRef + 2L) ::
        (idRef + 2L) :: (idRef + 3L) ::
        Literal(13L) :: idRef ::
        (idRef + 3L) :: (idRef + 4L) ::
        (idRef + 4L) :: (idRef + 5L)::
        Nil
     ).getMapValue(13L) as "a"
   )
    val optimized = Optimize execute rel
    val expected = baseOptimizedPlan.select(
      Coalesce(
        CreateMap(
          idRef :: (idRef + 1L) ::
            (idRef + 1L) :: (idRef + 2L) ::
            (idRef + 2L) :: (idRef + 3L) ::
            Nil
       ).getMapValue(13L) ::
        idRef ::
        Nil
     ) as "a"
   )
    comparePlans(optimized, expected)
  }

  test("simplify map ops, dynamic lookup, dynamic keys, lookup is equivalent to one of the keys") {
    val rel = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
        (idRef + 1L) :: (idRef + 2L) ::
        (idRef + 2L) :: (idRef + 3L) ::
        (idRef + 3L) :: (idRef + 4L) ::
        (idRef + 4L) :: (idRef + 5L)::
        Nil
     ).getMapValue(idRef + 3L) as "a"
   )
    val optimized = Optimize execute rel
    val expected = baseOptimizedPlan.select(
      Coalesce(
        CreateMap(
          idRef :: (idRef + 1L) ::
            (idRef + 1L) :: (idRef + 2L) ::
            (idRef + 2L) :: (idRef + 3L) ::
            Nil
       ).getMapValue(idRef + 3L) ::
          (idRef + 4L) ::
          Nil
     ) as "a"
   )
    comparePlans(optimized, expected)
  }

  test("simplify map ops, no positive match") {
    val rel = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
        (idRef + 1L) :: (idRef + 2L) ::
        (idRef + 2L) :: (idRef + 3L) ::
        (idRef + 3L) :: (idRef + 4L) ::
        (idRef + 4L) :: (idRef + 5L)::
        Nil
     ).getMapValue(idRef + 30L) as "a"
   )
    val optimized = Optimize execute rel
    val expected = rel
    comparePlans(optimized, expected)
  }

  test("simplify map ops, constant lookup, mixed keys, eliminated constants") {
    val rel = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
        (idRef + 1L) :: (idRef + 2L) ::
        (idRef + 2L) :: (idRef + 3L) ::
        Literal(14L) :: idRef ::
        (idRef + 3L) :: (idRef + 4L) ::
        (idRef + 4L) :: (idRef + 5L)::
        Nil
     ).getMapValue(13L) as "a"
   )
    val optimized = Optimize execute rel
    val expected = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
          (idRef + 1L) :: (idRef + 2L) ::
          (idRef + 2L) :: (idRef + 3L) ::
          (idRef + 3L) :: (idRef + 4L) ::
          (idRef + 4L) :: (idRef + 5L) ::
          Nil
     ).getMapValue(13L) as "a"
   )
    comparePlans(optimized, expected)
  }

  test("simplify map ops, potential dynamic match with null value + an absolute constant match") {
    val rel = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
          (idRef + 1L) :: (idRef + 2L) ::
          (idRef + 2L) :: Literal.create(null, LongType) ::
          Literal(2L) :: idRef ::
          (idRef + 3L) :: (idRef + 4L) ::
          (idRef + 4L) :: (idRef + 5L) ::
          Nil
      ).getMapValue( 2L ) as "a"
    )
    val optimized = Optimize execute rel
    val expected = baseOptimizedPlan.select(
      CreateMap(
        idRef :: (idRef + 1L) ::
          // these two are possible matches, we can't tell untill runtime
          (idRef + 1L) :: (idRef + 2L) ::
          (idRef + 2L) :: Literal.create(null, LongType) ::
          // this is a definite match (two constants),
          // but it cannot override a potential match with (idRef + 2L),
          // which is exactly what [[Coalesce]] would do in this case.
          Literal(2L) :: idRef ::
          Nil
      ).getMapValue( 2L ) as "a"
    )
    comparePlans(optimized, expected)
  }
}
