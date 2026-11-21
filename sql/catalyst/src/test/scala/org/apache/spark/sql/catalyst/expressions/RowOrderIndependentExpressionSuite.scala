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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, PullOutNondeterministic, SimpleAnalyzer}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, OneRowRelation, Project, Sort, Window}
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{DataType, IntegerType, ObjectType, StringType}
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * Test-only expression simulating a row-order-independent nondeterministic UDF.
 */
case class RowOrderIndepTestExpr(
    child: Expression)(
    val counter: AtomicInteger = new AtomicInteger(0))
  extends UnaryExpression with CodegenFallback with UserDefinedExpression {

  override def name: String = "row_order_indep_test"

  override lazy val deterministic: Boolean = false

  override lazy val rowOrderIndependent: Boolean = child.rowOrderIndependent

  override def dataType: DataType = child.dataType

  override protected def nullSafeEval(input: Any): Any = {
    counter.incrementAndGet()
    input
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)(counter)

  override protected final def otherCopyArgs: Seq[AnyRef] = counter :: Nil
}

/**
 * Test-only expression overriding `deterministic = false` without overriding `rowOrderIndependent`,
 * exercising the conservative `ClassValue` fallback.
 */
case class CustomNondetUnaryTestExpr(child: Expression)
  extends UnaryExpression with CodegenFallback {
  override lazy val deterministic: Boolean = false
  override def dataType: DataType = child.dataType
  override protected def nullSafeEval(input: Any): Any = input
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class CustomNondetLeafTestExpr() extends LeafExpression with CodegenFallback {
  override lazy val deterministic: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType
  override def eval(input: InternalRow): Any = 1
}

case class CustomNondetBinaryTestExpr(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {
  override lazy val deterministic: Boolean = false
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(input1: Any, input2: Any): Any = 1
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

/**
 * Test-only deterministic throwable expression simulating an ANSI runtime failure on 0.
 */
case class ThrowableDetTestExpr(child: Expression)
  extends UnaryExpression with CodegenFallback {
  override lazy val throwable: Boolean = true
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(input: Any): Any = {
    val v = input.asInstanceOf[Int]
    if (v == 0) {
      throw new ArithmeticException("Simulated division by zero")
    }
    10 / v
  }
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/**
 * Test-only row-order-independent nondeterministic expression that throws on non-positive input.
 */
case class ThrowableRowOrderIndepTestExpr(child: Expression)
  extends UnaryExpression with CodegenFallback {
  override lazy val deterministic: Boolean = false
  override lazy val rowOrderIndependent: Boolean = child.rowOrderIndependent
  override lazy val throwable: Boolean = true
  override def dataType: DataType = IntegerType
  override protected def nullSafeEval(input: Any): Any = {
    val v = input.asInstanceOf[Int]
    if (v <= 0) {
      throw new ArithmeticException("Simulated UDF error on non-positive input")
    }
    v
  }
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

private[expressions] class LinkageMissingDep

private[expressions] case class LinkageBrokenTestExpr(child: Expression)
  extends UnaryExpression with Unevaluable {
  def methodWithMissingParam(dep: LinkageMissingDep): Int = 0
  override def dataType: DataType = child.dataType
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

object StaticInvokeTestHelper {
  def echo(v: Int): Int = v
  def combine(x: Int, y: Int): Int = x + y
}

case class DummyV2ScalarFunc(deterministicFlag: Boolean) extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "dummy_v2_func"
  override def canonicalName(): String = "dummy_v2_func"
  override def isDeterministic(): Boolean = deterministicFlag
  override def produceResult(input: InternalRow): Int = input.getInt(0)
}

class RowOrderIndependentExpressionSuite extends SparkFunSuite {

  test("built-in Nondeterministic expressions have rowOrderIndependent == false") {
    val exprs: Seq[Expression] = Seq(
      Rand(0),
      Randn(0),
      Uuid(Some(0L)),
      Shuffle(CreateArray(Seq(Literal(1), Literal(2))), Some(0L)),
      MonotonicallyIncreasingID(),
      SparkPartitionID(),
      InputFileName(),
      InputFileBlockStart(),
      InputFileBlockLength(),
      CurrentBatchTimestamp(0L, org.apache.spark.sql.types.TimestampType),
      new Uniform(Literal(1), Literal(10), Literal(0L)),
      CallMethodViaReflection(Seq(Literal("java.lang.Math"), Literal("abs"), Literal(-1)))
    )
    exprs.foreach { e =>
      assert(!e.deterministic, s"Expected deterministic == false for $e")
      assert(!e.rowOrderIndependent, s"Expected rowOrderIndependent == false for $e")
    }
  }

  test("pure combinators propagate rowOrderIndependent while unsafe children block it") {
    val a = $"a".int
    val s = $"s".string
    val roiInt = RowOrderIndepTestExpr(a)()
    val roiStr = RowOrderIndepTestExpr(s)()

    val combinators: Seq[Expression] = Seq(
      Upper(roiStr),
      Add(roiInt, Literal(1)),
      Alias(roiInt, "c")(),
      Cast(roiInt, StringType),
      Coalesce(Seq(roiInt, Literal(0))),
      CaseWhen(Seq((a > 0, roiInt)), Literal(0)),
      CreateNamedStruct(Seq(Literal("k"), roiInt)),
      And(roiInt > 0, a > 1),
      Or(roiInt > 0, a > 1),
      EqualTo(roiInt, Literal(1))
    )
    combinators.foreach { e =>
      assert(!e.deterministic, s"Expected deterministic == false for $e")
      assert(e.rowOrderIndependent, s"Expected rowOrderIndependent == true for $e")
    }

    val unsafeCombined = RowOrderIndepTestExpr(MonotonicallyIncreasingID())()
    assert(!unsafeCombined.deterministic)
    assert(!unsafeCombined.rowOrderIndependent)

    val unsafeUpper = Upper(Cast(MonotonicallyIncreasingID(), StringType))
    assert(!unsafeUpper.deterministic)
    assert(!unsafeUpper.rowOrderIndependent)
  }

  test("If, CaseWhen, Coalesce, and LambdaFunction propagation") {
    val a = $"a".int
    val roi = RowOrderIndepTestExpr(a)()
    val unsafe = MonotonicallyIncreasingID()
    val x = UnresolvedNamedLambdaVariable("x" :: Nil)

    val safeExprs: Seq[Expression] = Seq(
      If(a > 0, roi, Literal(0)),
      If(roi > 0, a, Literal(0)),
      CaseWhen(Seq((a > 0, Literal(1)), (roi > 0, Literal(2))), Some(Literal(3))),
      Coalesce(Seq(Literal(null, IntegerType), roi)),
      LambdaFunction(x + roi, Seq(x))
    )
    safeExprs.foreach { e =>
      assert(!e.deterministic, s"Expected deterministic == false for $e")
      assert(e.rowOrderIndependent, s"Expected rowOrderIndependent == true for $e")
    }

    val unsafeExprs: Seq[Expression] = Seq(
      If(a > 0, roi, Cast(unsafe, IntegerType)),
      CaseWhen(Seq((a > 0, roi)), Some(Cast(unsafe, IntegerType))),
      Coalesce(Seq(roi, Cast(unsafe, IntegerType))),
      LambdaFunction(x + Cast(unsafe, IntegerType), Seq(x)),
      LambdaFunction(roi, Seq(x), hidden = true) match {
        case l => LambdaFunction(l.function + Cast(unsafe, IntegerType), l.arguments)
      }
    )
    unsafeExprs.foreach { e =>
      assert(!e.deterministic, s"Expected deterministic == false for $e")
      assert(!e.rowOrderIndependent, s"Expected rowOrderIndependent == false for $e")
    }
  }

  test("custom third-party !deterministic exprs default to rowOrderIndependent == false") {
    val extUdf = ExternalUserDefinedFunction(
      name = Some("ext_nondet"),
      workerSpec = UDFWorkerSpecification.getDefaultInstance,
      payload = Array.emptyByteArray,
      dataType = IntegerType,
      children = Seq($"a".int),
      udfDeterministic = false,
      udfNullable = true)

    val customExprs: Seq[Expression] = Seq(
      CustomNondetLeafTestExpr(),
      CustomNondetUnaryTestExpr($"a".int),
      CustomNondetBinaryTestExpr($"a".int, $"b".int),
      extUdf
    )
    customExprs.foreach { e =>
      assert(!e.deterministic, s"Expected deterministic == false for $e")
      assert(!e.rowOrderIndependent, s"Expected rowOrderIndependent == false for $e")
    }
  }

  test("LinkageError during ClassValue lookup falls back conservatively to deterministic") {
    val targetName = classOf[LinkageBrokenTestExpr].getName
    val missingName = classOf[LinkageMissingDep].getName
    val parentLoader = getClass.getClassLoader

    // Positive control: with LinkageMissingDep loadable, LinkageBrokenTestExpr inherits
    // Expression.deterministic and propagates rowOrderIndependent from its child.
    val normalRoi = new LinkageBrokenTestExpr(RowOrderIndepTestExpr(Literal(1))())
    assert(!normalRoi.deterministic)
    assert(normalRoi.rowOrderIndependent)

    // Child-first loader that hides LinkageMissingDep while loading LinkageBrokenTestExpr fresh,
    // so Class.getMethod("deterministic") fails with LinkageError (NoClassDefFoundError).
    val brokenLoader = new ClassLoader(parentLoader) {
      override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
        if (name == missingName) {
          throw new ClassNotFoundException(name)
        } else if (name == targetName) {
          val loaded = findLoadedClass(name)
          val cls = if (loaded != null) {
            loaded
          } else {
            val resource = name.replace('.', '/') + ".class"
            val stream = parentLoader.getResourceAsStream(resource)
            assert(stream != null, s"Could not read class bytes for $resource")
            val bytes = try {
              stream.readAllBytes()
            } finally {
              stream.close()
            }
            defineClass(name, bytes, 0, bytes.length)
          }
          if (resolve) resolveClass(cls)
          cls
        } else {
          super.loadClass(name, resolve)
        }
      }
    }

    val brokenCls = brokenLoader.loadClass(targetName)
    // Precondition: reflective lookup on the broken class raises LinkageError.
    intercept[LinkageError](brokenCls.getMethod("deterministic"))

    val ctor = brokenCls.getDeclaredConstructor(classOf[Expression])
    val brokenWithRoiChild =
      ctor.newInstance(RowOrderIndepTestExpr(Literal(1))()).asInstanceOf[Expression]
    assert(!brokenWithRoiChild.deterministic)
    // Conservative fallback: treated as overriding deterministic => rowOrderIndependent = false.
    assert(!brokenWithRoiChild.rowOrderIndependent)

    val brokenWithDetChild = ctor.newInstance(Literal(1)).asInstanceOf[Expression]
    assert(brokenWithDetChild.deterministic)
    assert(brokenWithDetChild.rowOrderIndependent)
  }

  test("explicit overrides on StaticInvoke, Invoke, and PlanExpression") {
    val a = $"a".int
    val roi = RowOrderIndepTestExpr(a)()

    val detStaticInvoke = StaticInvoke(
      StaticInvokeTestHelper.getClass,
      IntegerType,
      "echo",
      Seq(roi),
      isDeterministic = true)
    assert(!detStaticInvoke.deterministic)
    assert(detStaticInvoke.rowOrderIndependent)

    val nondetStaticInvoke = StaticInvoke(
      StaticInvokeTestHelper.getClass,
      IntegerType,
      "echo",
      Seq(a),
      isDeterministic = false)
    assert(!nondetStaticInvoke.deterministic)
    assert(!nondetStaticInvoke.rowOrderIndependent)

    val roiStaticInvoke = StaticInvoke(
      StaticInvokeTestHelper.getClass,
      IntegerType,
      "echo",
      Seq(a),
      isDeterministic = false,
      isRowOrderIndependent = true)
    assert(!roiStaticInvoke.deterministic)
    assert(roiStaticInvoke.rowOrderIndependent)

    // Compositional safety: StaticInvoke(isRowOrderIndependent = true) over stateful Rand(1)
    // must still have rowOrderIndependent == false.
    val roiStaticInvokeOverRand = StaticInvoke(
      StaticInvokeTestHelper.getClass,
      IntegerType,
      "echo",
      Seq(Cast(Rand(1), IntegerType)),
      isDeterministic = false,
      isRowOrderIndependent = true)
    assert(!roiStaticInvokeOverRand.deterministic)
    assert(!roiStaticInvokeOverRand.rowOrderIndependent)

    // StaticInvoke(isRowOrderIndependent = true) over another ROI expression composes safely.
    val roiStaticInvokeOverRoi = StaticInvoke(
      StaticInvokeTestHelper.getClass,
      IntegerType,
      "echo",
      Seq(roi),
      isDeterministic = false,
      isRowOrderIndependent = true)
    assert(!roiStaticInvokeOverRoi.deterministic)
    assert(roiStaticInvokeOverRoi.rowOrderIndependent)

    // Deterministic StaticInvoke over Rand(1) must also yield rowOrderIndependent == false.
    val detStaticInvokeOverRand = StaticInvoke(
      StaticInvokeTestHelper.getClass,
      IntegerType,
      "echo",
      Seq(Cast(Rand(1), IntegerType)),
      isDeterministic = true)
    assert(!detStaticInvokeOverRand.deterministic)
    assert(!detStaticInvokeOverRand.rowOrderIndependent)

    // Default StaticInvoke (isRowOrderIndependent = false) omits both unset trailing defaults
    // in argString/simpleString, preserving existing plan string output.
    assert(!nondetStaticInvoke.argString(maxFields = 20).contains("None"))
    assert(roiStaticInvoke.argString(maxFields = 20).endsWith(", true"))

    val detInvoke = Invoke(
      Literal.create(StaticInvokeTestHelper, ObjectType(StaticInvokeTestHelper.getClass)),
      "echo",
      IntegerType,
      Seq(roi),
      isDeterministic = true)
    assert(!detInvoke.deterministic)
    assert(detInvoke.rowOrderIndependent)

    val nondetInvoke = Invoke(
      Literal.create(StaticInvokeTestHelper, ObjectType(StaticInvokeTestHelper.getClass)),
      "echo",
      IntegerType,
      Seq(a),
      isDeterministic = false)
    assert(!nondetInvoke.deterministic)
    assert(!nondetInvoke.rowOrderIndependent)

    // PlanExpression checks both children.forall(_.rowOrderIndependent) and plan.deterministic.
    val detPlan = Project(Seq(Literal(1).as("x")), OneRowRelation())
    val nondetPlan = Project(Seq(Rand(0).as("x")), OneRowRelation())

    assert(ScalarSubquery(detPlan).rowOrderIndependent)
    assert(!ScalarSubquery(nondetPlan).rowOrderIndependent)
    assert(!Exists(nondetPlan).rowOrderIndependent)
    assert(!InSubquery(Seq(a), ListQuery(nondetPlan)).rowOrderIndependent)

    val scalarSubqueryWithRoiOuter = ScalarSubquery(detPlan, outerAttrs = Seq(roi))
    assert(!scalarSubqueryWithRoiOuter.deterministic)
    assert(scalarSubqueryWithRoiOuter.rowOrderIndependent)

    val scalarSubqueryWithNondetOuter =
      ScalarSubquery(detPlan, outerAttrs = Seq(MonotonicallyIncreasingID()))
    assert(!scalarSubqueryWithNondetOuter.rowOrderIndependent)
  }

  test("FunctionRegistry expressions honor deterministic/rowOrderIndependent contract") {
    val detArgs: Seq[Expression] = Seq.fill(4)(Literal(1))
    val roiArgs: Seq[Expression] = Seq.fill(4)(RowOrderIndepTestExpr(Literal(1))())
    val unsafeArgs: Seq[Expression] = Seq.fill(4)(CustomNondetLeafTestExpr())

    // Probe arities 0..4 across FunctionRegistry expressions that accept integer inputs.
    FunctionRegistry.expressions.foreach { case (name, (_, builder)) =>
      val builtWithDet = (0 to 4).view.flatMap { arity =>
        scala.util.Try(arity -> builder(detArgs.take(arity))).toOption
      }.headOption

      builtWithDet.foreach { case (arity, detExpr) =>
        if (detExpr.deterministic) {
          assert(detExpr.rowOrderIndependent,
            s"Built-in expression '$name' (${detExpr.getClass.getName}) has " +
              "deterministic == true but rowOrderIndependent == false")

          if (arity > 0) {
            scala.util.Try(builder(roiArgs.take(arity))).toOption.foreach { roiExpr =>
              if (!roiExpr.deterministic) {
                assert(roiExpr.rowOrderIndependent,
                  s"Built-in expression '$name' (${roiExpr.getClass.getName}) blocked " +
                    "rowOrderIndependent propagation from a !deterministic && " +
                    "rowOrderIndependent child")
              }
            }
            scala.util.Try(builder(unsafeArgs.take(arity))).toOption.foreach { unsafeExpr =>
              if (!unsafeExpr.deterministic) {
                assert(!unsafeExpr.rowOrderIndependent,
                  s"Built-in expression '$name' (${unsafeExpr.getClass.getName}) leaked " +
                    "rowOrderIndependent == true with an unsafe nondeterministic child")
              }
            }
          }
        } else {
          assert(!detExpr.rowOrderIndependent,
            s"Built-in expression '$name' (${detExpr.getClass.getName}) has " +
              "deterministic == false but rowOrderIndependent == true")
        }
      }
    }
  }

  test("EquivalentExpressions and ExpressionSet never merge/dedup " +
    "rowOrderIndependent nondeterministic exprs") {
    val equiv = new EquivalentExpressions
    val a = $"a".int
    val expr1 = RowOrderIndepTestExpr(a)()
    val expr2 = RowOrderIndepTestExpr(a)()
    assert(!equiv.addExpr(expr1))
    assert(!equiv.addExpr(expr2))
    assert(equiv.getExprState(expr1).isEmpty)
    assert(equiv.getCommonSubexpressions.isEmpty)

    val exprSet = ExpressionSet(Seq(expr1, expr2))
    assert(exprSet.size == 2)
    assert(!exprSet.contains(expr1))

    val pred1 = expr1 > 0
    val pred2 = expr2 > 0
    val predSet = ExpressionSet(Seq(pred1, pred2))
    assert(predSet.size == 2)
    assert(!predSet.contains(pred1))
  }

  test("PullOutNondeterministic extracts rowOrderIndependent exprs from " +
    "Aggregate, Sort, and Window") {
    val a = $"a".int
    val b = $"b".int
    val relation = LocalRelation(a, b)
    val roiExpr = RowOrderIndepTestExpr(a)()

    // Aggregate grouping expression
    val aggPlan = relation.groupBy(roiExpr)(roiExpr.as("c"))
    val pulledAgg = PullOutNondeterministic(aggPlan).asInstanceOf[Aggregate]
    assert(pulledAgg.groupingExpressions.forall(_.deterministic))
    assert(pulledAgg.child.isInstanceOf[Project])
    assert(pulledAgg.child.expressions.exists(_.exists(_ == roiExpr)))

    // Sort order expression
    val sortPlan = relation.sortBy(SortOrder(roiExpr, Ascending))
    val pulledSort = PullOutNondeterministic(sortPlan).asInstanceOf[Project]
    val innerSort = pulledSort.child.asInstanceOf[Sort]
    assert(innerSort.order.forall(_.deterministic))
    assert(innerSort.child.isInstanceOf[Project])
    assert(innerSort.child.expressions.exists(_.exists(_ == roiExpr)))

    // Window expression (ExtractWindowExpressions + PullOutNondeterministic via SimpleAnalyzer)
    val winExpr = WindowExpression(
      Count(b).toAggregateExpression(),
      WindowSpecDefinition(Seq(roiExpr), Seq(SortOrder(b, Ascending)), UnspecifiedFrame))
    val winPlan = SimpleAnalyzer.execute(relation.select(a, winExpr.as("w")))
    val windowNode = winPlan.collectFirst { case w: Window => w }
    assert(windowNode.isDefined)
    assert(windowNode.get.partitionSpec.forall(_.deterministic))
    assert(windowNode.get.orderSpec.forall(_.deterministic))
    assert(windowNode.get.windowExpressions.forall(_.deterministic))
    assert(windowNode.get.child.isInstanceOf[Project])
    assert(windowNode.get.child.expressions.exists(_.exists(_ == roiExpr)))
  }
}
