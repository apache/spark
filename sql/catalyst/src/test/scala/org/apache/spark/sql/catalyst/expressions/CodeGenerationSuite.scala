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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, RandomDataGenerator}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

  test("SPARK-8443: split wide projections into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = List.fill(length)(EqualTo(Literal(1), Literal(1)))
    val plan = GenerateMutableProjection.generate(expressions)()
    val actual = plan(new GenericMutableRow(length)).toSeq(expressions.map(_.dataType))
    val expected = Seq.fill(length)(true)

    if (!checkResult(actual, expected)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-13242: case-when expression with large number of branches (or cases)") {
    val cases = 50
    val clauses = 20

    // Generate an individual case
    def generateCase(n: Int): Seq[Expression] = {
      val condition = (1 to clauses)
        .map(c => EqualTo(BoundReference(0, StringType, false), Literal(s"$c:$n")))
        .reduceLeft[Expression]((l, r) => Or(l, r))
      Seq(condition, Literal(n))
    }

    val expression = CaseWhen((1 to cases).flatMap(generateCase(_)))

    val plan = GenerateMutableProjection.generate(Seq(expression))()
    val input = new GenericMutableRow(Array[Any](UTF8String.fromString(s"${clauses}:${cases}")))
    val actual = plan(input).toSeq(Seq(expression.dataType))

    assert(actual(0) == cases)
  }


  test("test generated safe and unsafe projection") {
    val schema = new StructType(Array(
      StructField("a", StringType, true),
      StructField("b", IntegerType, true),
      StructField("c", new StructType(Array(
        StructField("aa", StringType, true),
        StructField("bb", IntegerType, true)
      )), true),
      StructField("d", new StructType(Array(
        StructField("a", new StructType(Array(
          StructField("b", StringType, true),
          StructField("", IntegerType, true)
        )), true)
      )), true)
    ))
    val row = Row("a", 1, Row("b", 2), Row(Row("c", 3)))
    val lit = Literal.create(row, schema)
    val internalRow = lit.value.asInstanceOf[InternalRow]

    val unsafeProj = UnsafeProjection.create(schema)
    val unsafeRow: UnsafeRow = unsafeProj(internalRow)
    assert(unsafeRow.getUTF8String(0) === UTF8String.fromString("a"))
    assert(unsafeRow.getInt(1) === 1)
    assert(unsafeRow.getStruct(2, 2).getUTF8String(0) === UTF8String.fromString("b"))
    assert(unsafeRow.getStruct(2, 2).getInt(1) === 2)
    assert(unsafeRow.getStruct(3, 1).getStruct(0, 2).getUTF8String(0) ===
      UTF8String.fromString("c"))
    assert(unsafeRow.getStruct(3, 1).getStruct(0, 2).getInt(1) === 3)

    val fromUnsafe = FromUnsafeProjection(schema)
    val internalRow2 = fromUnsafe(unsafeRow)
    assert(internalRow === internalRow2)

    // update unsafeRow should not affect internalRow2
    unsafeRow.setInt(1, 10)
    unsafeRow.getStruct(2, 2).setInt(1, 10)
    unsafeRow.getStruct(3, 1).getStruct(0, 2).setInt(1, 4)
    assert(internalRow === internalRow2)
  }

  test("*/ in the data") {
    // When */ appears in a comment block (i.e. in /**/), code gen will break.
    // So, in Expression and CodegenFallback, we escape */ to \*\/.
    checkEvaluation(
      EqualTo(BoundReference(0, StringType, false), Literal.create("*/", StringType)),
      true,
      InternalRow(UTF8String.fromString("*/")))
  }

  test("\\u in the data") {
    // When \ u appears in a comment block (i.e. in /**/), code gen will break.
    // So, in Expression and CodegenFallback, we escape \ u to \\u.
    checkEvaluation(
      EqualTo(BoundReference(0, StringType, false), Literal.create("\\u", StringType)),
      true,
      InternalRow(UTF8String.fromString("\\u")))
  }

  test("check compilation error doesn't occur caused by specific literal") {
    // The end of comment (*/) should be escaped.
    GenerateUnsafeProjection.generate(
      Literal.create("*/Compilation error occurs/*", StringType) :: Nil)

    // `\u002A` is `*` and `\u002F` is `/`
    // so if the end of comment consists of those characters in queries, we need to escape them.
    GenerateUnsafeProjection.generate(
      Literal.create("\\u002A/Compilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\\\u002A/Compilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\u002a/Compilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\\\u002a/Compilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("*\\u002FCompilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("*\\\\u002FCompilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("*\\002fCompilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("*\\\\002fCompilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\002A\\002FCompilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\\\002A\\002FCompilation error occurs/*", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\002A\\\\002FCompilation error occurs/*", StringType) :: Nil)

    // \ u002X is an invalid unicode literal so it should be escaped.
    GenerateUnsafeProjection.generate(
      Literal.create("\\u002X/Compilation error occurs", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\\\u002X/Compilation error occurs", StringType) :: Nil)

    // \ u001 is an invalid unicode literal so it should be escaped.
    GenerateUnsafeProjection.generate(
      Literal.create("\\u001/Compilation error occurs", StringType) :: Nil)
    GenerateUnsafeProjection.generate(
      Literal.create("\\\\u001/Compilation error occurs", StringType) :: Nil)
  }
}
