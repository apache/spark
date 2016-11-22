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
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, CreateNamedStructLike, CreateNamedStructUnsafe, GetStructField, Literal}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}
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

  object LocalOptimizer extends
    Optimizer( sqlContext.sparkSession.sessionState.catalog, sqlContext.conf) {
    override val batches =
      super.batches :+ Batch( "experimental", FixedPoint(100), GetOnCreateStruct)
  }

  def checkOptimizedPlan( plan : LogicalPlan, expected : LogicalPlan ): Unit = {
    val optimizedPlan = LocalOptimizer execute plan
    comparePlans( optimizedPlan, expected)
  }
}
