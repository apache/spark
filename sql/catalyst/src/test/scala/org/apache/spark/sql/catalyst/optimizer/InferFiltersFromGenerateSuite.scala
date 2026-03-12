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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

class InferFiltersFromGenerateSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Infer Filters", Once, InferFiltersFromGenerate) :: Nil
  }

  val testRelation = LocalRelation($"a".array(StructType(Seq(
    StructField("x", IntegerType),
    StructField("y", IntegerType)
  ))), $"c1".string, $"c2".string, $"c3".int)

  Seq(Explode(_), PosExplode(_), Inline(_)).foreach { f =>
    val generator = f($"a")
    test("Infer filters from " + generator) {
      val originalQuery = testRelation.generate(generator).analyze
      val correctAnswer = testRelation
        .where(IsNotNull($"a") && Size($"a") > 0)
        .generate(generator)
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, correctAnswer)
    }

    test("Don't infer duplicate filters from " + generator) {
      val originalQuery = testRelation
        .where(IsNotNull($"a") && Size($"a") > 0)
        .generate(generator)
        .analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }

    test("Don't infer filters from outer " + generator) {
      val originalQuery = testRelation.generate(generator, outer = true).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }

    val foldableExplode = f(CreateArray(Seq(
      CreateStruct(Seq(Literal(0), Literal(1))),
      CreateStruct(Seq(Literal(2), Literal(3)))
    )))
    test("Don't infer filters from " + foldableExplode) {
      val originalQuery = testRelation.generate(foldableExplode).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }

    val generatorWithFromJson = f(JsonToStructs(
      ArrayType(new StructType().add("s", "string")),
      Map.empty,
      $"c1"))
    test("SPARK-37392: Don't infer filters from " + generatorWithFromJson) {
      val originalQuery = testRelation.generate(generatorWithFromJson).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }

    val returnSchema = ArrayType(StructType(Seq(
      StructField("x", IntegerType),
      StructField("y", StringType)
    )))
    val fakeUDF = ScalaUDF(
      (i: Int) => Array(Row.fromSeq(Seq(1, "a")), Row.fromSeq(Seq(2, "b"))),
      returnSchema, $"c3" :: Nil, Nil)
    val generatorWithUDF = f(fakeUDF)
    test("SPARK-36715: Don't infer filters from " + generatorWithUDF) {
      val originalQuery = testRelation.generate(generatorWithUDF).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  Seq(Explode(_), PosExplode(_)).foreach { f =>
    val createArrayExplode = f(CreateArray(Seq($"c1")))
    test("SPARK-33544: Don't infer filters from " + createArrayExplode) {
      val originalQuery = testRelation.generate(createArrayExplode).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
    val createMapExplode = f(CreateMap(Seq($"c1", $"c2")))
    test("SPARK-33544: Don't infer filters from " + createMapExplode) {
      val originalQuery = testRelation.generate(createMapExplode).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }

  Seq(Inline(_)).foreach { f =>
    val createArrayStructExplode = f(CreateArray(Seq(CreateStruct(Seq($"c1")))))
    test("SPARK-33544: Don't infer filters from " + createArrayStructExplode) {
      val originalQuery = testRelation.generate(createArrayStructExplode).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, originalQuery)
    }
  }
}
