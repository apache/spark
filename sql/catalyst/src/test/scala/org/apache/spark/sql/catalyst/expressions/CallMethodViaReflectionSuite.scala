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
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SPARK_DOC_ROOT, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLType
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenFallback, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLConf
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ThreadUtils

/** A static class for testing purpose. */
object ReflectStaticClass {
  def method1(): String = "m1"
  def method2(v1: Int): String = "m" + v1
  def method3(v1: java.lang.Integer): String = "m" + v1
  def method4(v1: Int, v2: String): String = "m" + v1 + v2
  def method5(v1: Int, v2: Int): String = "m" + v1 + v2
}

/**
 * An argument whose evaluation for the row with id 1 blocks until the row with id 2 has been
 * evaluated. Placed after another argument, it holds a `CallMethodViaReflection` evaluation
 * between filling its argument buffer and making the call, so a buffer shared with a concurrent
 * evaluation is observable.
 */
case class ReflectBlockingArgument(
    firstEvaluationStarted: CountDownLatch,
    secondEvaluationStarted: CountDownLatch) extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType
  override def eval(input: InternalRow): Any = {
    if (input.getInt(0) == 1) {
      firstEvaluationStarted.countDown()
      assert(secondEvaluationStarted.await(10, TimeUnit.SECONDS))
    } else {
      secondEvaluationStarted.countDown()
    }
    0
  }
}

/** A non-static class for testing purpose. */
class ReflectDynamicClass {
  def method1(): String = "m1"
}

/**
 * Test suite for [[CallMethodViaReflection]] and its companion object.
 */
class CallMethodViaReflectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  import CallMethodViaReflection._

  // Get rid of the $ so we are getting the companion object's name.
  private val staticClassName = ReflectStaticClass.getClass.getName.stripSuffix("$")
  private val dynamicClassName = classOf[ReflectDynamicClass].getName

  /**
   * Runs `f` with the given regular expression patterns set as the reflect allow list. The config
   * is static, so it cannot be set via `withSQLConf`; we install a fresh `SQLConf` instead. Using
   * `setConfString` keeps the `checkValue` validation in the path.
   */
  private def withAllowList(patterns: String*)(f: => Unit): Unit = {
    val conf = new SQLConf()
    conf.setConfString(StaticSQLConf.REFLECT_ALLOW_LIST.key, patterns.mkString(","))
    SQLConf.withExistingConf(conf)(f)
  }

  test("findMethod via reflection for static methods") {
    assert(findMethod(staticClassName, "method1", Seq.empty).exists(_.getName == "method1"))
    assert(findMethod(staticClassName, "method2", Seq(IntegerType)).isDefined)
    assert(findMethod(staticClassName, "method3", Seq(IntegerType)).isDefined)
    assert(findMethod(staticClassName, "method4", Seq(IntegerType, StringType)).isDefined)
  }

  test("findMethod for a JDK library") {
    assert(findMethod(classOf[java.util.UUID].getName, "randomUUID", Seq.empty).isDefined)
  }

  test("class not found") {
    val wrongClassName = "some-random-class"
    val ret = createExpr(wrongClassName, "method").checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_CLASS_TYPE",
        messageParameters = Map("className" -> wrongClassName)
      )
    )
  }

  test("method not found because name does not match") {
    val wrongMethodName = "notfoundmethod"
    val ret = createExpr(staticClassName, wrongMethodName).checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_STATIC_METHOD",
        messageParameters = Map("methodName" -> wrongMethodName, "className" -> staticClassName)
      )
    )
  }

  test("method not found because there is no static method") {
    val wrongMethodName = "method1"
    val ret = createExpr(dynamicClassName, wrongMethodName).checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_STATIC_METHOD",
        messageParameters = Map("methodName" -> wrongMethodName, "className" -> dynamicClassName)
      )
    )
  }

  test("input type checking") {
    checkError(
      exception = intercept[AnalysisException] {
        CallMethodViaReflection(Seq.empty).checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`reflect`",
        "expectedNum" -> "> 1",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )
    checkError(
      exception = intercept[AnalysisException] {
        CallMethodViaReflection(Seq(Literal(staticClassName))).checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`reflect`",
        "expectedNum" -> "> 1",
        "actualNum" -> "1",
        "docroot" -> SPARK_DOC_ROOT)
    )
    assert(CallMethodViaReflection(
      Seq(Literal(staticClassName), Literal(1))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "`method`",
          "inputType" -> "\"STRING\"",
          "inputExpr" -> "\"1\"")
      )
    )
    assert(createExpr(staticClassName, "method1").checkInputDataTypes().isSuccess)
  }

  test("unsupported type checking") {
    val ret = createExpr(staticClassName, "method1", new Timestamp(1)).checkInputDataTypes()
    assert(ret.isFailure)
    assert(ret ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(2),
          "requiredType" -> toSQLType(
            TypeCollection(BooleanType, ByteType, ShortType,
              IntegerType, LongType, FloatType, DoubleType, StringType)),
          "inputSql" -> "\"TIMESTAMP '1969-12-31 16:00:00.001'\"",
          "inputType" -> "\"TIMESTAMP\""
        )
      )
    )
  }

  test("invoking methods using acceptable types") {
    checkEvaluation(createExpr(staticClassName, "method1"), "m1")
    checkEvaluation(createExpr(staticClassName, "method2", 2), "m2")
    checkEvaluation(createExpr(staticClassName, "method3", 3), "m3")
    checkEvaluation(createExpr(staticClassName, "method4", 4, "four"), "m4four")
  }

  test("SPARK-57448: allow list restricts reflective calls when set") {
    def methodNotAllowed(methodName: String): DataTypeMismatch =
      DataTypeMismatch(
        errorSubClass = "METHOD_NOT_ALLOWED",
        messageParameters = Map(
          "methodName" -> methodName,
          "className" -> staticClassName,
          "config" -> toSQLConf(StaticSQLConf.REFLECT_ALLOW_LIST.key))
      )

    // An empty allow list (the default) allows every call.
    assert(createExpr(staticClassName, "method1").checkInputDataTypes().isSuccess)

    // When set, only methods whose canonical name matches a pattern are allowed.
    withAllowList(s"$staticClassName.method1") {
      assert(createExpr(staticClassName, "method1").checkInputDataTypes().isSuccess)
      assert(createExpr(staticClassName, "method2", 2).checkInputDataTypes() ==
        methodNotAllowed("method2"))
    }

    // Regular expression patterns are supported.
    withAllowList("java\\.util\\..*") {
      assert(createExpr("java.util.UUID", "randomUUID").checkInputDataTypes().isSuccess)
    }
  }

  test("SPARK-57448: invalid regular expression in the allow list is rejected") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        withAllowList("(unclosed") {}
      },
      condition = "INVALID_CONF_VALUE.REQUIREMENT",
      parameters = Map(
        "confName" -> StaticSQLConf.REFLECT_ALLOW_LIST.key,
        "confValue" -> "(unclosed",
        "confRequirement" -> "Every entry must be a valid regular expression."))
  }

  test("escaping of class and method names") {
    GenerateUnsafeProjection.generate(
      CallMethodViaReflection(Seq(Literal("\"quote"), Literal("\"quote"), Literal(null))) :: Nil)
  }

  test("SPARK-58209: CallMethodViaReflection is stateful and produces fresh copies") {
    val expr = createExpr(staticClassName, "method2", 5)
    assert(expr.stateful, "CallMethodViaReflection.stateful should be true")
    val copy = expr.freshCopyIfContainsStatefulExpression()
    assert(copy ne expr,
      "freshCopyIfContainsStatefulExpression should return a new instance " +
        "for CallMethodViaReflection")
    copy.asInstanceOf[Nondeterministic].initialize(0)
    assert(copy.eval(InternalRow.empty) === UTF8String.fromString("m5"))
  }

  test("SPARK-58209: fresh copies do not share the reflection argument buffer") {
    val firstEvaluationStarted = new CountDownLatch(1)
    val secondEvaluationStarted = new CountDownLatch(1)
    // The first argument is written into the buffer before the blocking second argument is
    // evaluated, so on a shared buffer the second evaluation overwrites the first one's value.
    val expr = CallMethodViaReflection(Seq(
      Literal(staticClassName),
      Literal("method5"),
      BoundReference(0, IntegerType, nullable = false),
      ReflectBlockingArgument(firstEvaluationStarted, secondEvaluationStarted)))
    def freshEvaluator(): Expression = {
      val copy = expr.freshCopyIfContainsStatefulExpression()
      copy.asInstanceOf[Nondeterministic].initialize(0)
      copy
    }
    val firstEvaluator = freshEvaluator()
    val secondEvaluator = freshEvaluator()

    val executor = ThreadUtils.newDaemonFixedThreadPool(2, "call-method-via-reflection-test")
    val executionContext = ExecutionContext.fromExecutorService(executor)
    try {
      val firstResult = Future(firstEvaluator.eval(InternalRow(1)))(executionContext)
      assert(firstEvaluationStarted.await(10, TimeUnit.SECONDS))
      val secondResult = Future(secondEvaluator.eval(InternalRow(2)))(executionContext)

      assert(ThreadUtils.awaitResult(secondResult, 10.seconds) === UTF8String.fromString("m20"))
      assert(ThreadUtils.awaitResult(firstResult, 10.seconds) === UTF8String.fromString("m10"))
    } finally {
      executor.shutdownNow()
    }
  }

  private def createExpr(className: String, methodName: String, args: Any*) = {
    CallMethodViaReflection(
      Literal.create(className, StringType) +:
      Literal.create(methodName, StringType) +:
      args.map(Literal.apply)
    )
  }
}
