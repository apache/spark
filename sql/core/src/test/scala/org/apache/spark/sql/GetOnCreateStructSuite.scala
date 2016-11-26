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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateArray, CreateMap, CreateNamedStruct, CreateNamedStructLike, CreateNamedStructUnsafe, Expression, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, ShouldMatchers, Suite, Tag}

import scala.collection.immutable.IndexedSeq

/**
* Created by eyalf on 11/4/2016.
*/
class GetOnCreateStructSuite extends PlanTest with SharedSQLContext with ShouldMatchers{
  import testImplicits._

  lazy val baseRelation = sqlContext.range( 1L, 1000L)

  abstract class Tests(useAdditionalOptimizer : Boolean) extends SparkFunSuite{
    def checkOptimizedPlan( plan : LogicalPlan, expected : LogicalPlan ): Unit = {
      val optimizedPlan = if (useAdditionalOptimizer) LocalOptimizer.execute(plan) else plan
      comparePlans( optimizedPlan, expected)
    }

    test( "explicit" ) {
      val rel = baseRelation.select(
        functions.struct( $"id" as "att" ).getField("att") as "outerAtt"
      )
      rel.schema should have size(1)
      rel.schema shouldEqual
        StructType( StructField( "outerAtt", LongType, nullable = false ) :: Nil )

      val optimized = rel.queryExecution.optimizedPlan
      val expected = baseRelation.select( $"id" as "outerAtt" )

      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }
    test( "explicit - deduced att name") {
      val rel = baseRelation.select(
        functions.struct( $"id" as "att" ).getField("att")
      )
      rel.schema should have size(1)
      rel.schema shouldEqual
        StructType(
          StructField( "named_struct(att, id AS `att`).att", LongType, nullable = false ) :: Nil
        )

      val optimized = rel.queryExecution.optimizedPlan
      val expected = baseRelation.select( $"id" as "named_struct(att, id AS `att`).att" )

      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }

    test( "collapsed" ) {
      val rel = baseRelation.select(
        functions.struct( $"id" as "att" ) as "struct1"
      )
      rel.schema should have size(1)
      rel.schema shouldEqual
        StructType(
          StructField(
            "struct1",
            StructType(
              StructField(
                "att",
                LongType,
                false
              ) :: Nil
            ),
            false
          ) :: Nil
        )

      val rel2 = rel.select( $"struct1".getField("att").as("struct1Att"))

      rel2.schema shouldEqual
        StructType(
          StructField( "struct1Att", LongType, false ) :: Nil
        )

      val optimized = rel2.queryExecution.optimizedPlan
      val expected = baseRelation.select( $"id" as "struct1Att" )

      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }

    test( "collapsed2" ) {
      val rel = baseRelation.select(
        functions.struct( $"id" as "att1", ($"id" * $"id") as "att2" ) as "struct1"
      )
      rel.schema shouldEqual
        StructType(
          StructField(
            "struct1",
            StructType(
              StructField(
                "att1",
                LongType,
                false
              ) ::
              StructField(
                "att2",
                LongType,
                false
              ) :: Nil
            ),
            false
          ) :: Nil
        )

      val rel2 = rel.select(
        $"struct1".getField("att1").as("struct1Att1"),
        $"struct1".getField("att2").as("struct1Att2"))

      rel2.schema shouldEqual
        StructType(
          StructField( "struct1Att1", LongType, false ) ::
          StructField( "struct1Att2", LongType, false ) ::
          Nil
        )

      val optimized = rel2.queryExecution.optimizedPlan
      val expected = baseRelation.select( $"id" as "struct1Att1", ($"id" * $"id") as "struct1Att2" )

      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }

    test( "collapsed2 - deduced names" ) {
      val rel = baseRelation.select(
        functions.struct( $"id" as "att1", ($"id" * $"id") as "att2" ) as "struct1"
      )
      rel.schema shouldEqual
        StructType(
          StructField(
            "struct1",
            StructType(
              StructField(
                "att1",
                LongType,
                false
              ) ::
                StructField(
                  "att2",
                  LongType,
                  false
                ) :: Nil
            ),
            false
          ) :: Nil
        )

      val rel2 = rel.select(
        $"struct1".getField("att1"),
        $"struct1".getField("att2"))

      rel2.schema shouldEqual
        StructType(
          StructField( "struct1.att1", LongType, false ) ::
            StructField( "struct1.att2", LongType, false ) ::
            Nil
        )

      val optimized = rel2.queryExecution.optimizedPlan
      val expected =
        baseRelation.select(
          $"id" as "struct1.att1",
          ($"id" * $"id") as "struct1.att2" )

      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }

    test( "simplified array ops") {
      val rel = baseRelation.select(
        functions.array(
          functions.struct( $"id" as "att1", ($"id" * $"id") as "att2" ),
          functions.struct( $"id" + 1 as "att1", (($"id" + 1) * ($"id") + 1) as "att2" )
        ) as "arr"
      )
      rel.schema shouldEqual
        StructType(
          StructField(
            "arr",
            ArrayType(
              StructType(
                StructField(
                  "att1",
                  LongType,
                  false
                ) ::
                StructField(
                  "att2",
                  LongType,
                  false
                ) :: Nil
              ),
              false
            ),
            nullable = false
          ) :: Nil
      )

      val rel2 = rel.select(
        $"arr".getField("att1") as "a1",
        $"arr".getItem(1) as "a2",
        $"arr".getItem(1).getField("att1") as "a3",
        $"arr".getField("att1").getItem(1) as "a4"
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
        baseRelation.select(
          functions.array( $"id", $"id" + 1 ) as "a1",
          functions.struct( $"id" + 1 as "att1", (($"id" + 1) * ($"id") + 1) as "att2" ) as "a2",
          $"id" + 1 as "a3",
          $"id" + 1 as "a4"
        )
      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }

    test( "simplify map ops" ) {
      val rel = baseRelation.select(
        functions.map(
          functions.lit( "r1" ),
          functions.struct( $"id" as "att1"),
          functions.lit( "r2" ),
          functions.struct( ($"id" + 1L) as "att1")
        ) as "m"
      )
      rel.schema shouldEqual
        StructType(
          StructField(
            "m",
            MapType(
              StringType,
              StructType(
                StructField(
                  "att1",
                  LongType,
                  nullable = false
                )
                :: Nil
              ),
              valueContainsNull = false
            ),
            nullable = false
          )
          :: Nil
        )

      val rel2 = rel.select(
        $"m".getField("r1") as "a1",
        $"m".getField("r1").getField("att1") as "a2",
        $"m".getField("r32") as "a3",
        $"m".getField("r32").getField("att1") as "a4"
      )
      val optimized = rel2.queryExecution.optimizedPlan

      val expected = baseRelation.select(
        functions.struct( $"id" as "att1") as "a1",
        $"id" as "a2",
        functions.lit(null).cast(
          StructType(
            StructField("att1", LongType, nullable = false) :: Nil
          )
        ) as "a3",
        functions.lit(null).cast(LongType) as "a4"
      )
      checkOptimizedPlan( optimized, expected.queryExecution.optimizedPlan )
    }

    test( "GetArrayStructFields nullable bug") {
      val rel = baseRelation.select(
        functions.array(
          functions.struct( $"id" as "att1", functions.lit(null) as "n"),
          functions.struct( $"id" + 1 as "att1", functions.lit(null) as "n")
        ) as "arr"
      )

      rel.schema shouldEqual
        StructType(
          StructField(
            "arr",
            ArrayType(
              StructType(
                StructField(
                  "att1",
                  LongType,
                  false
                ) ::
                  StructField(
                    "n",
                    NullType,
                    true
                  ) :: Nil
              ),
              false
            ),
            nullable = false
          ) :: Nil
        )

      val rel2 = rel.select(
        $"arr".getField("n") as "a_n"
      )
      rel2.schema shouldEqual
        StructType(
          StructField(
            "a_n",
            ArrayType(
              NullType,
              /**
                * here's the bug,
                * array members are all CreateNamedStrucywhich is non-nullable,
                * the selected attribute however is nullable,
                * hence the resulting return type should indicate this attribute as nullable.
                * the fix is rather simple:
                * at [[org.apache.spark.sql.catalyst.expressions.GetArrayStructFields#dataType()]]
                * use {{{containsNull || field.nullable}}}
                */
              true
            ),
            nullable = false
          ) :: Nil
        )
    }
  }

  object Std extends Tests(false)
  object Optimized extends Tests(true)

  override def nestedSuites: IndexedSeq[Suite] = super.nestedSuites :+ Std :+ Optimized

  object GetOnCreateStruct extends Rule[LogicalPlan]{
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformExpressionsUp{
        case GetStructField( CreateNamedStructLike(_, vals), ordinal, _ ) =>
          val value = vals(ordinal)
          // this might loose alias in case of top level expression
          value
      }
    }
  }
  object SimplifyCreateArrayOps extends Rule[LogicalPlan]{
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformExpressionsUp{
        case GetArrayStructFields(CreateArray(elems), field, ordinal, numFields, containsNull) =>
          def getStructField( elem : Expression ) = {
            GetStructField( elem, ordinal, Some(field.name) )
          }
          CreateArray( elems.map(getStructField) )
        case ga @ GetArrayItem( CreateArray(elems), IntegerLiteral( idx ) ) =>
          if ( idx >= 0 && idx < elems.size ) {
            elems(idx)
          } else {
            Cast( Literal( null), ga.dataType )
          }
      }
    }
  }

  object SimplifyCreateMapOps extends Rule[LogicalPlan]{
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transformExpressionsUp{
        // attempt to unfold 'constant' key extraction,
        // this enables other optimizations to take place.
        case gmv @ GetMapValue(cm @ CreateMap(elems), key @ Literal(v, t)) =>
        if ( cm.keys.contains( key ) ) {
          val idx = cm.keys.indexOf(key)
          cm.values(idx)
        } else {
          Cast( Literal( null ), gmv.dataType)
        }
      }
    }
  }

  object LocalOptimizer extends
    Optimizer( sqlContext.sparkSession.sessionState.catalog, sqlContext.conf) {
    val additionalRules = SimplifyCreateMapOps :: SimplifyCreateArrayOps :: GetOnCreateStruct :: Nil
    override val batches = super.batches map{
      case b if b.name == "Operator Optimizations" =>
        Batch( b.name, b.strategy, b.rules ++ additionalRules : _*)
      case b => b
    }
  }

  def checkOptimizedPlan( plan : LogicalPlan, expected : LogicalPlan ): Unit = {
    val optimizedPlan = LocalOptimizer execute plan
    comparePlans( optimizedPlan, expected)
  }
}
