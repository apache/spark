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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CreateArray, CreateNamedStruct, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.scalatest.ShouldMatchers

/**
* Created by eyalf on 11/4/2016.
* SPARK-18601 discusses simplification direct access to complex types creators.
* i.e. {{{create_named_struct(square, `x` * `x`).square}}} can be simplified to {{{`x` * `x`}}}.
* sam applies to create_array and create_map
*/
class Spark18601Suite extends PlanTest with SharedSQLContext with ShouldMatchers{
  lazy val baseRelation = sqlContext.range( 1L, 1000L)
  lazy val baseOptimizedPlan = baseRelation.queryExecution.optimizedPlan

  val idRef = ('id).long.notNull
  val idRefColumn = Column( "id" )
  val struct1RefColumn = Column( "struct1" )

  test("explicit") {
    val rel = baseRelation.select(
      functions.struct( idRefColumn as "att" ).getField("att") as "outerAtt"
    )
    rel.schema shouldEqual
      StructType( StructField( "outerAtt", LongType, nullable = false ) :: Nil )

    val optimized = rel.queryExecution.optimizedPlan

    val expected = baseOptimizedPlan.select(idRef as "outerAtt")

    comparePlans(optimized, expected)
  }

  test("explicit - deduced att name") {
    val rel = baseRelation.select(functions.struct( idRefColumn as "att" ).getField("att"))
    rel.schema shouldEqual
      StructType(
        StructField( "named_struct(att, id AS `att`).att", LongType, nullable = false ) :: Nil
      )

    val optimized = rel.queryExecution.optimizedPlan

    val expected = baseOptimizedPlan.select(idRef as "named_struct(att, id AS `att`).att")

    comparePlans(optimized, expected)
  }

  test("collapsed") {
    val rel = baseRelation.select(
      functions.struct( idRefColumn as "att" ) as "struct1"
    )
    rel.schema shouldEqual
      StructType(
        StructField(
          "struct1",
          StructType(StructField("att", LongType, false) :: Nil),
          false
        ) :: Nil
      )

    val rel2 = rel.select( struct1RefColumn.getField("att").as("struct1Att"))

    rel2.schema shouldEqual
      StructType(
        StructField( "struct1Att", LongType, false ) :: Nil
      )

    val optimized = rel2.queryExecution.optimizedPlan
    val expected =
      baseOptimizedPlan.select(idRef as "struct1Att"  )

    comparePlans( optimized, expected )
  }

  test("collapsed2") {
    val rel = baseRelation.select(
      functions.struct( idRefColumn as "att1", (idRefColumn * idRefColumn) as "att2" ) as "struct1"
    )
    rel.schema shouldEqual
      StructType(
        StructField(
          "struct1",
          StructType(
            StructField("att1", LongType, false) ::
            StructField("att2", LongType, false ) :: Nil
          ),
          false
        ) :: Nil
      )

    val rel2 = rel.select(
      struct1RefColumn.getField("att1").as("struct1Att1"),
      struct1RefColumn.getField("att2").as("struct1Att2"))

    rel2.schema shouldEqual
      StructType(
        StructField( "struct1Att1", LongType, false ) ::
        StructField( "struct1Att2", LongType, false ) ::
        Nil
      )

    val optimized = rel2.queryExecution.optimizedPlan
    val expected =
      baseOptimizedPlan.select(
        idRef as "struct1Att1",
        (idRef * idRef) as "struct1Att2"
      )

    comparePlans( optimized, expected )
  }

  test("collapsed2 - deduced names") {
    val rel = baseRelation.select(
      functions.struct( idRefColumn as "att1", (idRefColumn * idRefColumn) as "att2" ) as "struct1"
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

    val rel2 = rel.select(
      struct1RefColumn.getField("att1"),
      struct1RefColumn.getField("att2"))

    rel2.schema shouldEqual
      StructType(
        StructField( "struct1.att1", LongType, false ) ::
        StructField( "struct1.att2", LongType, false ) ::
        Nil
      )

    val optimized = rel2.queryExecution.optimizedPlan
    val expected =
      baseOptimizedPlan.select(
        idRef as "struct1.att1",
        (idRef * idRef) as "struct1.att2"
      )

    comparePlans( optimized, expected )
  }

  test("simplified array ops") {
    val arrRefColumn = Column("arr")
    val rel = baseRelation.select(
      functions.array(
        functions.struct( idRefColumn as "att1", (idRefColumn * idRefColumn) as "att2" ),
        functions.struct(
          idRefColumn + 1 as "att1",
          ((idRefColumn + 1) * (idRefColumn + 1) ) as "att2"
        )
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

    val rel2 = rel.select(
      arrRefColumn.getField("att1") as "a1",
      arrRefColumn.getItem(1) as "a2",
      arrRefColumn.getItem(1).getField("att1") as "a3",
      arrRefColumn.getField("att1").getItem(1) as "a4"
    )

    rel2.schema shouldEqual
      StructType(
        StructField( "a1", ArrayType(LongType, false), nullable = false) ::
          StructField( "a2",
            StructType(
              StructField( "att1", LongType, nullable = false ) ::
              StructField( "att2", LongType, nullable = false ) ::
              Nil
            ),
            nullable = true
          ) ::
          StructField( "a3", LongType, nullable = true ) ::
          StructField( "a4", LongType, nullable = true ) ::
          Nil
      )

    val optimized = rel2.queryExecution.optimizedPlan
    val expected =
      baseOptimizedPlan.select(
        CreateArray( idRef :: idRef + 1L :: Nil ) as "a1",
        CreateNamedStruct(
          "att1" :: (idRef + 1L) ::
          Literal("att2") :: ((idRef + 1L) * (idRef + 1L)) ::
          Nil
        ) as "a2",
        (idRef + 1L) as "a3",
        (idRef + 1L) as "a4"
      )
    comparePlans( optimized, expected )
  }

  test("simplify map ops") {
    val mRefColumn = Column("m")
    val rel = baseRelation.select(
      functions.map(
        functions.lit( "r1" ),
        functions.struct( idRefColumn as "att1"),
        functions.lit( "r2" ),
        functions.struct( (idRefColumn + 1L) as "att1")
      ) as "m"
    )
    rel.schema shouldEqual
      StructType(
        StructField(
          "m",
          MapType(
            StringType,
            StructType( StructField ("att1", LongType, nullable = false) :: Nil),
            valueContainsNull = false
          ),
          nullable = false
        )
        :: Nil
      )

    val rel2 = rel.select(
      mRefColumn.getField("r1") as "a1",
      mRefColumn.getField("r1").getField("att1") as "a2",
      mRefColumn.getField("r32") as "a3",
      mRefColumn.getField("r32").getField("att1") as "a4"
    )
    val optimized = rel2.queryExecution.optimizedPlan

    val expected =
      baseOptimizedPlan.select(
        CreateNamedStruct( "att1" :: idRef:: Nil ) as "a1",
        idRef as "a2",
        Literal.create(
          null,
          StructType(
            StructField("att1", LongType, nullable = false) :: Nil
          )
        ) as "a3",
        Literal.create(null, LongType ) as "a4"
      )
    comparePlans( optimized, expected )
  }
}
