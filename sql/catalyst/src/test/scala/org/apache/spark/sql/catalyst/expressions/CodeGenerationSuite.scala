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

package org.apache.spark.sql.catalyst.expressions

import scala.math._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataTypeTestUtils, NullType, StructField, StructType}

/**
 * Additional tests for code generation.
 */
class CodeGenerationSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("multithreaded eval") {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val futures = (1 to 20).map { _ =>
      future {
        GeneratePredicate.generate(EqualTo(Literal(1), Literal(1)))
        GenerateProjection.generate(EqualTo(Literal(1), Literal(1)) :: Nil)
        GenerateMutableProjection.generate(EqualTo(Literal(1), Literal(1)) :: Nil)
        GenerateOrdering.generate(Add(Literal(1), Literal(1)).asc :: Nil)
      }
    }

    futures.foreach(Await.result(_, 10.seconds))
  }

  // Test GenerateOrdering for all common types. For each type, we construct random input rows that
  // contain two columns of that type, then for pairs of randomly-generated rows we check that
  // GenerateOrdering agrees with RowOrdering.
  (DataTypeTestUtils.atomicTypes ++ Set(NullType)).foreach { dataType =>
    test(s"GenerateOrdering with $dataType") {
      val rowOrdering = RowOrdering.forSchema(Seq(dataType, dataType))
      val genOrdering = GenerateOrdering.generate(
        BoundReference(0, dataType, nullable = true).asc ::
          BoundReference(1, dataType, nullable = true).asc :: Nil)
      val rowType = StructType(
        StructField("a", dataType, nullable = true) ::
          StructField("b", dataType, nullable = true) :: Nil)
      val maybeDataGenerator = RandomDataGenerator.forType(rowType, nullable = false)
      assume(maybeDataGenerator.isDefined)
      val randGenerator = maybeDataGenerator.get
      val toCatalyst = CatalystTypeConverters.createToCatalystConverter(rowType)
      for (_ <- 1 to 50) {
        val a = toCatalyst(randGenerator()).asInstanceOf[InternalRow]
        val b = toCatalyst(randGenerator()).asInstanceOf[InternalRow]
        withClue(s"a = $a, b = $b") {
          assert(genOrdering.compare(a, a) === 0)
          assert(genOrdering.compare(b, b) === 0)
          assert(rowOrdering.compare(a, a) === 0)
          assert(rowOrdering.compare(b, b) === 0)
          assert(signum(genOrdering.compare(a, b)) === -1 * signum(genOrdering.compare(b, a)))
          assert(signum(rowOrdering.compare(a, b)) === -1 * signum(rowOrdering.compare(b, a)))
          assert(
            signum(rowOrdering.compare(a, b)) === signum(genOrdering.compare(a, b)),
            "Generated and non-generated orderings should agree")
        }
      }
    }
  }

  test("SPARK-8443: split wide projections into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = List.fill(length)(EqualTo(Literal(1), Literal(1)))
    val plan = GenerateMutableProjection.generate(expressions)()
    val actual = plan(new GenericMutableRow(length)).toSeq
    val expected = Seq.fill(length)(true)

    if (!checkResult(actual, expected)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }
}
