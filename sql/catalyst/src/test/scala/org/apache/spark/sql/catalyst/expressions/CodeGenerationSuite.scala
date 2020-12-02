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

import java.sql.Timestamp

import scala.math.Ordering

import org.apache.spark.SparkFunSuite
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.LA
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ThreadUtils

/**
 * Additional tests for code generation.
 */
class CodeGenerationSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("multithreaded eval") {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val futures = (1 to 20).map { _ =>
      Future {
        GeneratePredicate.generate(EqualTo(Literal(1), Literal(1)))
        GenerateMutableProjection.generate(EqualTo(Literal(1), Literal(1)) :: Nil)
        GenerateOrdering.generate(Add(Literal(1), Literal(1)).asc :: Nil)
      }
    }

    futures.foreach(ThreadUtils.awaitResult(_, 10.seconds))
  }

  test("metrics are recorded on compile") {
    val startCount1 = CodegenMetrics.METRIC_COMPILATION_TIME.getCount()
    val startCount2 = CodegenMetrics.METRIC_SOURCE_CODE_SIZE.getCount()
    val startCount3 = CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.getCount()
    val startCount4 = CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.getCount()
    GenerateOrdering.generate(Add(Literal(123), Literal(1)).asc :: Nil)
    assert(CodegenMetrics.METRIC_COMPILATION_TIME.getCount() == startCount1 + 1)
    assert(CodegenMetrics.METRIC_SOURCE_CODE_SIZE.getCount() == startCount2 + 1)
    assert(CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.getCount() > startCount3)
    assert(CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.getCount() > startCount4)
  }

  test("SPARK-8443: split wide projections into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = List.fill(length)(EqualTo(Literal(1), Literal(1)))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    val expected = Seq.fill(length)(true)

    if (actual != expected) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-13242: case-when expression with large number of branches (or cases)") {
    val cases = 500
    val clauses = 20

    // Generate an individual case
    def generateCase(n: Int): (Expression, Expression) = {
      val condition = (1 to clauses)
        .map(c => EqualTo(BoundReference(0, StringType, false), Literal(s"$c:$n")))
        .reduceLeft[Expression]((l, r) => Or(l, r))
      (condition, Literal(n))
    }

    val expression = CaseWhen((1 to cases).map(generateCase))

    val plan = GenerateMutableProjection.generate(Seq(expression))
    val input = new GenericInternalRow(Array[Any](UTF8String.fromString(s"$clauses:$cases")))
    val actual = plan(input).toSeq(Seq(expression.dataType))

    assert(actual.head == cases)
  }

  test("SPARK-22543: split large if expressions into blocks due to JVM code size limit") {
    var strExpr: Expression = Literal("abc")
    for (_ <- 1 to 150) {
      strExpr = Decode(Encode(strExpr, "utf-8"), "utf-8")
    }

    val expressions = Seq(If(EqualTo(strExpr, strExpr), strExpr, strExpr))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(null).toSeq(expressions.map(_.dataType))
    assert(actual.length == 1)
    val expected = UTF8String.fromString("abc")

    if (!checkResult(actual.head, expected, expressions.head)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-14793: split wide array creation into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = Seq(CreateArray(List.fill(length)(EqualTo(Literal(1), Literal(1)))))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    assert(actual.length == 1)
    val expected = UnsafeArrayData.fromPrimitiveArray(Array.fill(length)(true))

    if (!checkResult(actual.head, expected, expressions.head)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-14793: split wide map creation into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = Seq(CreateMap(
      List.fill(length)(EqualTo(Literal(1), Literal(1))).zipWithIndex.flatMap {
        case (expr, i) => Seq(Literal(i), expr)
      }))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    assert(actual.length == 1)
    val expected = ArrayBasedMapData((0 until length).toArray, Array.fill(length)(true))

    if (!checkResult(actual.head, expected, expressions.head)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-14793: split wide struct creation into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = Seq(CreateStruct(List.fill(length)(EqualTo(Literal(1), Literal(1)))))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    val expected = Seq(InternalRow(Seq.fill(length)(true): _*))

    if (!checkResult(actual, expected, expressions.head)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-14793: split wide named struct creation into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = Seq(CreateNamedStruct(
      List.fill(length)(EqualTo(Literal(1), Literal(1))).flatMap {
        expr => Seq(Literal(expr.toString), expr)
      }))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    assert(actual.length == 1)
    val expected = InternalRow(Seq.fill(length)(true): _*)

    if (!checkResult(actual.head, expected, expressions.head)) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-14224: split wide external row creation into blocks due to JVM code size limit") {
    val length = 5000
    val schema = StructType(Seq.fill(length)(StructField("int", IntegerType)))
    val expressions = Seq(CreateExternalRow(Seq.fill(length)(Literal(1)), schema))
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    val expected = Seq(Row.fromSeq(Seq.fill(length)(1)))

    if (actual != expected) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-17702: split wide constructor into blocks due to JVM code size limit") {
    val length = 5000
    val expressions = Seq.fill(length) {
      ToUTCTimestamp(
        Literal.create(Timestamp.valueOf("2015-07-24 00:00:00"), TimestampType),
        Literal.create(LA.getId, StringType))
    }
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    val expected = Seq.fill(length)(
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-07-24 07:00:00")))

    if (actual != expected) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
  }

  test("SPARK-22226: group splitted expressions into one method per nested class") {
    val length = 10000
    val expressions = Seq.fill(length) {
      ToUTCTimestamp(
        Literal.create(Timestamp.valueOf("2017-10-10 00:00:00"), TimestampType),
        Literal.create(LA.getId, StringType))
    }
    val plan = GenerateMutableProjection.generate(expressions)
    val actual = plan(new GenericInternalRow(length)).toSeq(expressions.map(_.dataType))
    val expected = Seq.fill(length)(
      DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2017-10-10 07:00:00")))

    if (actual != expected) {
      fail(s"Incorrect Evaluation: expressions: $expressions, actual: $actual, expected: $expected")
    }
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

    val fromUnsafe = SafeProjection.create(schema)
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

  test("SPARK-17160: field names are properly escaped by GetExternalRowField") {
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    GenerateUnsafeProjection.generate(
      ValidateExternalType(
        GetExternalRowField(inputObject, index = 0, fieldName = "\"quote"), IntegerType) :: Nil)
  }

  test("SPARK-17160: field names are properly escaped by AssertTrue") {
    GenerateUnsafeProjection.generate(AssertTrue(Cast(Literal("\""), BooleanType)).child :: Nil)
  }

  test("should not apply common subexpression elimination on conditional expressions") {
    val row = InternalRow(null)
    val bound = BoundReference(0, IntegerType, true)
    val assertNotNull = AssertNotNull(bound)
    val expr = If(IsNull(bound), Literal(1), Add(assertNotNull, assertNotNull))
    val projection = GenerateUnsafeProjection.generate(
      Seq(expr), subexpressionEliminationEnabled = true)
    // should not throw exception
    projection(row)
  }

  test("SPARK-22226: splitExpressions should not generate codes beyond 64KB") {
    val colNumber = 10000
    val attrs = (1 to colNumber).map(colIndex => AttributeReference(s"_$colIndex", IntegerType)())
    val lit = Literal(1000)
    val exprs = attrs.flatMap { a =>
      Seq(If(lit < a, lit, a), sqrt(a))
    }
    UnsafeProjection.create(exprs, attrs)
  }

  test("SPARK-22543: split large predicates into blocks due to JVM code size limit") {
    val length = 600

    val input = new GenericInternalRow(length)
    val utf8Str = UTF8String.fromString(s"abc")
    for (i <- 0 until length) {
      input.update(i, utf8Str)
    }

    var exprOr: Expression = Literal(false)
    for (i <- 0 until length) {
      exprOr = Or(EqualTo(BoundReference(i, StringType, true), Literal(s"c$i")), exprOr)
    }

    val planOr = GenerateMutableProjection.generate(Seq(exprOr))
    val actualOr = planOr(input).toSeq(Seq(exprOr.dataType))
    assert(actualOr.length == 1)
    val expectedOr = false

    if (!checkResult(actualOr.head, expectedOr, exprOr)) {
      fail(s"Incorrect Evaluation: expressions: $exprOr, actual: $actualOr, expected: $expectedOr")
    }

    var exprAnd: Expression = Literal(true)
    for (i <- 0 until length) {
      exprAnd = And(EqualTo(BoundReference(i, StringType, true), Literal(s"c$i")), exprAnd)
    }

    val planAnd = GenerateMutableProjection.generate(Seq(exprAnd))
    val actualAnd = planAnd(input).toSeq(Seq(exprAnd.dataType))
    assert(actualAnd.length == 1)
    val expectedAnd = false

    if (!checkResult(actualAnd.head, expectedAnd, exprAnd)) {
      fail(
        s"Incorrect Evaluation: expressions: $exprAnd, actual: $actualAnd, expected: $expectedAnd")
    }
  }

  test("SPARK-22696: CreateExternalRow should not use global variables") {
    val ctx = new CodegenContext
    val schema = new StructType().add("a", IntegerType).add("b", StringType)
    CreateExternalRow(Seq(Literal(1), Literal("x")), schema).genCode(ctx)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("SPARK-22696: InitializeJavaBean should not use global variables") {
    val ctx = new CodegenContext
    InitializeJavaBean(Literal.fromObject(new java.util.LinkedList[Int]),
      Map("add" -> Literal(1))).genCode(ctx)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("SPARK-22716: addReferenceObj should not add mutable states") {
    val ctx = new CodegenContext
    val foo = new Object()
    ctx.addReferenceObj("foo", foo)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("SPARK-18016: define mutable states by using an array") {
    val ctx1 = new CodegenContext
    for (i <- 1 to CodeGenerator.OUTER_CLASS_VARIABLES_THRESHOLD + 10) {
      ctx1.addMutableState(CodeGenerator.JAVA_INT, "i", v => s"$v = $i;")
    }
    assert(ctx1.inlinedMutableStates.size == CodeGenerator.OUTER_CLASS_VARIABLES_THRESHOLD)
    // When the number of primitive type mutable states is over the threshold, others are
    // allocated into an array
    assert(ctx1.arrayCompactedMutableStates(CodeGenerator.JAVA_INT).arrayNames.size == 1)
    assert(ctx1.mutableStateInitCode.size == CodeGenerator.OUTER_CLASS_VARIABLES_THRESHOLD + 10)

    val ctx2 = new CodegenContext
    for (i <- 1 to CodeGenerator.MUTABLESTATEARRAY_SIZE_LIMIT + 10) {
      ctx2.addMutableState("InternalRow[]", "r", v => s"$v = new InternalRow[$i];")
    }
    // When the number of non-primitive type mutable states is over the threshold, others are
    // allocated into a new array
    assert(ctx2.inlinedMutableStates.isEmpty)
    assert(ctx2.arrayCompactedMutableStates("InternalRow[]").arrayNames.size == 2)
    assert(ctx2.arrayCompactedMutableStates("InternalRow[]").getCurrentIndex == 10)
    assert(ctx2.mutableStateInitCode.size == CodeGenerator.MUTABLESTATEARRAY_SIZE_LIMIT + 10)
  }

  test("SPARK-22750: addImmutableStateIfNotExists") {
    val ctx = new CodegenContext
    val mutableState1 = "field1"
    val mutableState2 = "field2"
    ctx.addImmutableStateIfNotExists("int", mutableState1)
    ctx.addImmutableStateIfNotExists("int", mutableState1)
    ctx.addImmutableStateIfNotExists("String", mutableState2)
    ctx.addImmutableStateIfNotExists("int", mutableState1)
    ctx.addImmutableStateIfNotExists("String", mutableState2)
    assert(ctx.inlinedMutableStates.length == 2)
  }

  test("SPARK-23628: calculateParamLength should compute properly the param length") {
    assert(CodeGenerator.calculateParamLength(Seq.range(0, 100).map(Literal(_))) == 101)
    assert(CodeGenerator.calculateParamLength(
      Seq.range(0, 100).map(x => Literal(x.toLong))) == 201)
  }

  test("SPARK-23760: CodegenContext.withSubExprEliminationExprs should save/restore correctly") {

    val ref = BoundReference(0, IntegerType, true)
    val add1 = Add(ref, ref)
    val add2 = Add(add1, add1)
    val dummy = SubExprEliminationState(
      JavaCode.variable("dummy", BooleanType),
      JavaCode.variable("dummy", BooleanType))

    // raw testing of basic functionality
    {
      val ctx = new CodegenContext
      val e = ref.genCode(ctx)
      // before
      ctx.subExprEliminationExprs += ref -> SubExprEliminationState(e.isNull, e.value)
      assert(ctx.subExprEliminationExprs.contains(ref))
      // call withSubExprEliminationExprs
      ctx.withSubExprEliminationExprs(Map(add1 -> dummy)) {
        assert(ctx.subExprEliminationExprs.contains(add1))
        assert(!ctx.subExprEliminationExprs.contains(ref))
        Seq.empty
      }
      // after
      assert(ctx.subExprEliminationExprs.nonEmpty)
      assert(ctx.subExprEliminationExprs.contains(ref))
      assert(!ctx.subExprEliminationExprs.contains(add1))
    }

    // emulate an actual codegen workload
    {
      val ctx = new CodegenContext
      // before
      ctx.generateExpressions(Seq(add2, add1), doSubexpressionElimination = true) // trigger CSE
      assert(ctx.subExprEliminationExprs.contains(add1))
      // call withSubExprEliminationExprs
      ctx.withSubExprEliminationExprs(Map(ref -> dummy)) {
        assert(ctx.subExprEliminationExprs.contains(ref))
        assert(!ctx.subExprEliminationExprs.contains(add1))
        Seq.empty
      }
      // after
      assert(ctx.subExprEliminationExprs.nonEmpty)
      assert(ctx.subExprEliminationExprs.contains(add1))
      assert(!ctx.subExprEliminationExprs.contains(ref))
    }
  }

  test("SPARK-23986: freshName can generate duplicated names") {
    val ctx = new CodegenContext
    val names1 = ctx.freshName("myName1") :: ctx.freshName("myName1") ::
      ctx.freshName("myName11") :: Nil
    assert(names1.distinct.length == 3)
    val names2 = ctx.freshName("a") :: ctx.freshName("a") ::
      ctx.freshName("a_1") :: ctx.freshName("a_0") :: Nil
    assert(names2.distinct.length == 4)
  }

  test("SPARK-25113: should log when there exists generated methods above HugeMethodLimit") {
    val appender = new LogAppender("huge method limit")
    withLogAppender(appender, loggerName = Some(classOf[CodeGenerator[_, _]].getName)) {
      val x = 42
      val expr = HugeCodeIntExpression(x)
      val proj = GenerateUnsafeProjection.generate(Seq(expr))
      val actual = proj(null)
      assert(actual.getInt(0) == x)
    }
    assert(appender.loggingEvents
      .exists(_.getRenderedMessage().contains("Generated method too long")))
  }

  test("SPARK-28916: subexrepssion elimination can cause 64kb code limit on UnsafeProjection") {
    val numOfExprs = 10000
    val exprs = (0 to numOfExprs).flatMap(colIndex =>
      Seq(Add(BoundReference(colIndex, DoubleType, true),
        BoundReference(numOfExprs + colIndex, DoubleType, true)),
        Add(BoundReference(colIndex, DoubleType, true),
          BoundReference(numOfExprs + colIndex, DoubleType, true))))
    // these should not fail to compile due to 64K limit
    GenerateUnsafeProjection.generate(exprs, true)
    GenerateMutableProjection.generate(exprs, true)
  }

  test("SPARK-32624: Use CodeGenerator.typeName() to fix byte[] compile issue") {
    val ctx = new CodegenContext
    val bytes = new Array[Byte](3)
    val refTerm = ctx.addReferenceObj("bytes", bytes)
    assert(refTerm == "((byte[]) references[0] /* bytes */)")
  }

  test("SPARK-32624: CodegenContext.addReferenceObj should work for nested Scala class") {
    // emulate TypeUtils.getInterpretedOrdering(StringType)
    val ctx = new CodegenContext
    val comparator = implicitly[Ordering[UTF8String]]
    val refTerm = ctx.addReferenceObj("comparator", comparator)

    // Expecting result:
    //   "((scala.math.LowPriorityOrderingImplicits$$anon$3) references[0] /* comparator */)"
    // Using lenient assertions to be resilient to annonymous class numbering changes
    assert(!refTerm.contains("null"))
    assert(refTerm.contains("scala.math.LowPriorityOrderingImplicits$$anon$"))
  }
}

case class HugeCodeIntExpression(value: Int) extends Expression {
  override def nullable: Boolean = true
  override def dataType: DataType = IntegerType
  override def children: Seq[Expression] = Nil
  override def eval(input: InternalRow): Any = value
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Assuming HugeMethodLimit to be 8000
    val HugeMethodLimit = CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT
    // A single "int dummyN = 0;" will be at least 2 bytes of bytecode:
    //   0: iconst_0
    //   1: istore_1
    // and it'll become bigger as the number of local variables increases.
    // So 4000 such dummy local variable definitions are sufficient to bump the bytecode size
    // of a generated method to above 8000 bytes.
    val hugeCode = (0 until (HugeMethodLimit / 2)).map(i => s"int dummy$i = 0;").mkString("\n")
    val code =
      code"""{
         |  $hugeCode
         |}
         |boolean ${ev.isNull} = false;
         |int ${ev.value} = $value;
       """.stripMargin
    ev.copy(code = code)
  }
}
