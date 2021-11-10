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

import java.util.{Locale, UUID}

import scala.concurrent.Future

import org.apache.spark.{MapOutputStatistics, SparkFunSuite, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Statistics, UnresolvedHint}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.COLUMN_BATCH_SIZE
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.types.{DataType, Decimal, IntegerType, LongType, Metadata, StructType}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test cases for the [[SparkSessionExtensions]].
 */
class SparkSessionExtensionSuite extends SparkFunSuite {
  private def create(
      builder: SparkSessionExtensionsProvider): Seq[SparkSessionExtensionsProvider] = Seq(builder)

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(
      builders: Seq[SparkSessionExtensionsProvider])(f: SparkSession => Unit): Unit = {
    val builder = SparkSession.builder().master("local[1]")
    builders.foreach(builder.withExtensions)
    val spark = builder.getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  test("inject analyzer rule") {
    withSession(Seq(_.injectResolutionRule(MyRule))) { session =>
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject post hoc resolution analyzer rule") {
    withSession(Seq(_.injectPostHocResolutionRule(MyRule))) { session =>
      assert(session.sessionState.analyzer.postHocResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject check analysis rule") {
    withSession(Seq(_.injectCheckRule(MyCheckRule))) { session =>
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
    }
  }

  test("inject optimizer rule") {
    withSession(Seq(_.injectOptimizerRule(MyRule))) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
    }
  }

  test("SPARK-33621: inject a pre CBO rule") {
    withSession(Seq(_.injectPreCBORule(MyRule))) { session =>
      assert(session.sessionState.optimizer.preCBORules.contains(MyRule(session)))
    }
  }

  test("inject spark planner strategy") {
    withSession(Seq(_.injectPlannerStrategy(MySparkStrategy))) { session =>
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject parser") {
    val extension = create { extensions =>
      extensions.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
    }
    withSession(extension) { session =>
      assert(session.sessionState.sqlParser === CatalystSqlParser)
    }
  }

  test("inject multiple rules") {
    withSession(Seq(_.injectOptimizerRule(MyRule),
        _.injectPlannerStrategy(MySparkStrategy))) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject stacked parsers") {
    val extension = create { extensions =>
      extensions.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
      extensions.injectParser(MyParser)
      extensions.injectParser(MyParser)
    }
    withSession(extension) { session =>
      val parser = MyParser(session, MyParser(session, CatalystSqlParser))
      assert(session.sessionState.sqlParser === parser)
    }
  }

  test("inject function") {
    val extensions = create { extensions =>
      extensions.injectFunction(MyExtensions.myFunction)
    }
    withSession(extensions) { session =>
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
    }
  }

  case class MyHintRule(spark: SparkSession) extends Rule[LogicalPlan] {
    val MY_HINT_NAME = Set("CONVERT_TO_EMPTY")

    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
      case h: UnresolvedHint if MY_HINT_NAME.contains(h.name.toUpperCase(Locale.ROOT)) =>
        LocalRelation(h.output, data = Seq.empty, isStreaming = h.isStreaming)
    }
  }

  test("inject custom hint rule") {
    withSession(Seq(_.injectPostHocResolutionRule(MyHintRule))) { session =>
      assert(
        session.range(1).hint("CONVERT_TO_EMPTY").logicalPlan.isInstanceOf[LocalRelation],
        "plan is expected to be a local relation"
      )
    }
  }

  test("inject adaptive query prep rule") {
    val extensions = create { extensions =>
      // inject rule that will run during AQE query stage preparation and will add custom tags
      // to the plan
      extensions.injectQueryStagePrepRule(session => MyQueryStagePrepRule())
      // inject rule that will run during AQE query stage optimization and will verify that the
      // custom tags were written in the preparation phase
      extensions.injectColumnar(session =>
        MyColumnarRule(MyNewQueryStageRule(), MyNewQueryStageRule()))
    }
    withSession(extensions) { session =>
      session.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, true)
      assert(session.sessionState.queryStagePrepRules.contains(MyQueryStagePrepRule()))
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(MyNewQueryStageRule(), MyNewQueryStageRule())))
      import session.sqlContext.implicits._
      val data = Seq((100L), (200L), (300L)).toDF("vals").repartition(1)
      val df = data.selectExpr("vals + 1")
      df.collect()
    }
  }

  test("inject columnar AQE on") {
    testInjectColumnar(true)
  }

  test("inject columnar AQE off") {
    testInjectColumnar(false)
  }

  private def testInjectColumnar(enableAQE: Boolean): Unit = {
    def collectPlanSteps(plan: SparkPlan): Seq[Int] = plan match {
      case a: AdaptiveSparkPlanExec =>
        assert(a.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
        collectPlanSteps(a.executedPlan)
      case _ => plan.collect {
        case _: ReplacedRowToColumnarExec => 1
        case _: ColumnarProjectExec => 10
        case _: ColumnarToRowExec => 100
        case s: QueryStageExec => collectPlanSteps(s.plan).sum
        case _: MyShuffleExchangeExec => 1000
        case _: MyBroadcastExchangeExec => 10000
      }
    }

    val extensions = create { extensions =>
      extensions.injectColumnar(session =>
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
    }
    withSession(extensions) { session =>
      session.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, enableAQE)
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
      import session.sqlContext.implicits._
      // perform a join to inject a broadcast exchange
      val left = Seq((1, 50L), (2, 100L), (3, 150L)).toDF("l1", "l2")
      val right = Seq((1, 50L), (2, 100L), (3, 150L)).toDF("r1", "r2")
      val data = left.join(right, $"l1" === $"r1")
        // repartitioning avoids having the add operation pushed up into the LocalTableScan
        .repartition(1)
      val df = data.selectExpr("l2 + r2")
      // execute the plan so that the final adaptive plan is available when AQE is on
      df.collect()
      val found = collectPlanSteps(df.queryExecution.executedPlan).sum
      // 1 MyBroadcastExchangeExec
      // 1 MyShuffleExchangeExec
      // 1 ColumnarToRowExec
      // 2 ColumnarProjectExec
      // 1 ReplacedRowToColumnarExec
      // so 11121 is expected.
      assert(found == 11121)

      // Verify that we get back the expected, wrong, result
      val result = df.collect()
      assert(result(0).getLong(0) == 101L) // Check that broken columnar Add was used.
      assert(result(1).getLong(0) == 201L)
      assert(result(2).getLong(0) == 301L)
    }
  }

  test("reset column vectors") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(COLUMN_BATCH_SIZE.key, 2)
      .withExtensions { extensions =>
        extensions.injectColumnar(session =>
          MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())) }
      .getOrCreate()

    try {
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
      import session.sqlContext.implicits._

      val input = Seq((100L), (200L), (300L))
      val data = input.toDF("vals").repartition(1)
      val df = data.selectExpr("vals + 1")
      val result = df.collect()
      assert(result sameElements input.map(x => Row(x + 2)))
    } finally {
      stop(session)
    }
  }

  test("use custom class for extensions") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, classOf[MyExtensions].getCanonicalName)
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.analyzer.postHocResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
      assert(session.sessionState.sqlParser.isInstanceOf[MyParser])
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
      assert(session.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
    } finally {
      stop(session)
    }
  }

  test("use multiple custom class for extensions in the specified order") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, Seq(
        classOf[MyExtensions2].getCanonicalName,
        classOf[MyExtensions].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.containsSlice(
        Seq(MySparkStrategy2(session), MySparkStrategy(session))))
      val orderedRules = Seq(MyRule2(session), MyRule(session))
      val orderedCheckRules = Seq(MyCheckRule2(session), MyCheckRule(session))
      val parser = MyParser(session, CatalystSqlParser)
      assert(session.sessionState.analyzer.extendedResolutionRules.containsSlice(orderedRules))
      assert(session.sessionState.analyzer.postHocResolutionRules.containsSlice(orderedRules))
      assert(session.sessionState.analyzer.extendedCheckRules.containsSlice(orderedCheckRules))
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).filter(orderedRules.contains)
        .containsSlice(orderedRules ++ orderedRules)) // The optimizer rules are duplicated
      assert(session.sessionState.sqlParser === parser)
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions2.myFunction._1).isDefined)
    } finally {
      stop(session)
    }
  }

  test("allow an extension to be duplicated") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, Seq(
        classOf[MyExtensions].getCanonicalName,
        classOf[MyExtensions].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.count(_ === MySparkStrategy(session)) === 2)
      assert(session.sessionState.analyzer.extendedResolutionRules.count(_ === MyRule(session)) ===
        2)
      assert(session.sessionState.analyzer.postHocResolutionRules.count(_ === MyRule(session)) ===
        2)
      assert(session.sessionState.analyzer.extendedCheckRules.count(_ === MyCheckRule(session)) ===
        2)
      assert(session.sessionState.optimizer.batches.flatMap(_.rules)
        .count(_ === MyRule(session)) === 4) // The optimizer rules are duplicated
      val outerParser = session.sessionState.sqlParser
      assert(outerParser.isInstanceOf[MyParser])
      assert(outerParser.asInstanceOf[MyParser].delegate.isInstanceOf[MyParser])
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
    } finally {
      stop(session)
    }
  }

  test("use the last registered function name when there are duplicates") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config(SPARK_SESSION_EXTENSIONS.key, Seq(
        classOf[MyExtensions2].getCanonicalName,
        classOf[MyExtensions2Duplicate].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      val lastRegistered = session.sessionState.functionRegistry
        .lookupFunction(FunctionIdentifier("myFunction2"))
      assert(lastRegistered.isDefined)
      assert(lastRegistered.get !== MyExtensions2.myFunction._2)
      assert(lastRegistered.get === MyExtensions2Duplicate.myFunction._2)
    } finally {
      stop(session)
    }
  }

  test("SPARK-35380: Loading extensions from ServiceLoader") {
    val builder = SparkSession.builder().master("local[1]")

    Seq(None, Some(classOf[YourExtensions].getName)).foreach { ext =>
      ext.foreach(builder.config(SPARK_SESSION_EXTENSIONS.key, _))
      val session = builder.getOrCreate()
      try {
        assert(session.sql("select get_fake_app_name()").head().getString(0) === "Fake App Name")
      } finally {
        stop(session)
      }
    }
  }

  test("SPARK-35673: user-defined hint and unrecognized hint in subquery") {
    withSession(Seq(_.injectPostHocResolutionRule(MyHintRule))) { session =>
      // unrecognized hint
      QueryTest.checkAnswer(
        session.sql(
          """
            |SELECT *
            |FROM (
            |    SELECT /*+ some_random_hint_that_does_not_exist */ 42
            |)
            |""".stripMargin),
        Row(42) :: Nil)

      // user-defined hint
      QueryTest.checkAnswer(
        session.sql(
          """
            |SELECT *
            |FROM (
            |    SELECT /*+ CONVERT_TO_EMPTY */ 42
            |)
            |""".stripMargin),
        Nil)
    }
  }

  test("SPARK-37202: temp view refers a inject function") {
    val extensions = create { extensions =>
      extensions.injectFunction(MyExtensions.myFunction)
    }
    withSession(extensions) { session =>
      session.sql("CREATE TEMP VIEW v AS SELECT myFunction(a) FROM VALUES(1), (2) t(a)")
      session.sql("SELECT * FROM v")
    }
  }
}

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

case class MyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan =
    delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)
}

object MyExtensions {

  val myFunction = (FunctionIdentifier("myFunction"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

case class CloseableColumnBatchIterator(itr: Iterator[ColumnarBatch],
    f: ColumnarBatch => ColumnarBatch) extends Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
    closeCurrentBatch()
  })

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = f(itr.next())
    cb
  }
}

object NoCloseColumnVector extends Logging {
  def wrapIfNeeded(cv: ColumnVector): NoCloseColumnVector = cv match {
    case ref: NoCloseColumnVector =>
      ref
    case vec => NoCloseColumnVector(vec)
  }
}

/**
 * Provide a ColumnVector so ColumnarExpression can close temporary values without
 * having to guess what type it really is.
 */
case class NoCloseColumnVector(wrapped: ColumnVector) extends ColumnVector(wrapped.dataType) {
  private var refCount = 1

  /**
   * Don't actually close the ColumnVector this wraps.  The producer of the vector will take
   * care of that.
   */
  override def close(): Unit = {
    // Empty
  }

  override def hasNull: Boolean = wrapped.hasNull

  override def numNulls(): Int = wrapped.numNulls

  override def isNullAt(rowId: Int): Boolean = wrapped.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = wrapped.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = wrapped.getByte(rowId)

  override def getShort(rowId: Int): Short = wrapped.getShort(rowId)

  override def getInt(rowId: Int): Int = wrapped.getInt(rowId)

  override def getLong(rowId: Int): Long = wrapped.getLong(rowId)

  override def getFloat(rowId: Int): Float = wrapped.getFloat(rowId)

  override def getDouble(rowId: Int): Double = wrapped.getDouble(rowId)

  override def getArray(rowId: Int): ColumnarArray = wrapped.getArray(rowId)

  override def getMap(ordinal: Int): ColumnarMap = wrapped.getMap(ordinal)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    wrapped.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = wrapped.getUTF8String(rowId)

  override def getBinary(rowId: Int): Array[Byte] = wrapped.getBinary(rowId)

  override def getChild(ordinal: Int): ColumnVector = wrapped.getChild(ordinal)
}

trait ColumnarExpression extends Expression with Serializable {
  /**
   * Returns true if this expression supports columnar processing through [[columnarEval]].
   */
  def supportsColumnar: Boolean = true

  /**
   * Returns the result of evaluating this expression on the entire
   * [[org.apache.spark.sql.vectorized.ColumnarBatch]]. The result of
   * calling this may be a single [[org.apache.spark.sql.vectorized.ColumnVector]] or a scalar
   * value. Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a [[org.apache.spark.sql.catalyst.expressions.Literal]] that
   * [[org.apache.spark.sql.catalyst.expressions.Add]] would have to be able to handle.
   *
   * By convention any [[org.apache.spark.sql.vectorized.ColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a [[org.apache.spark.sql.vectorized.ColumnarBatch]] and closing the batch or by closing the
   * vector directly if it is a temporary value.
   */
  def columnarEval(batch: ColumnarBatch): Any = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support mismatch")
  }

  // We need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarExpression]
  }

  override def hashCode(): Int = super.hashCode()
}

object ColumnarBindReferences extends Logging {

  // Mostly copied from BoundAttribute.scala so we can do columnar processing
  def bindReference[A <: ColumnarExpression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      val ordinal = input.indexOf(a.exprId)
      if (ordinal == -1) {
        if (allowFailures) {
          a
        } else {
          sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
      } else {
        new ColumnarBoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
    }.asInstanceOf[A]
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
  def bindReferences[A <: ColumnarExpression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    expressions.map(ColumnarBindReferences.bindReference(_, input))
  }
}

class ColumnarBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends BoundReference(ordinal, dataType, nullable) with ColumnarExpression {

  override def columnarEval(batch: ColumnarBatch): Any = {
    // Because of the convention that the returned ColumnVector must be closed by the
    // caller we wrap this column vector so a close is a NOOP, and let the original source
    // of the vector close it.
    NoCloseColumnVector.wrapIfNeeded(batch.column(ordinal))
  }
}

class ColumnarAlias(child: ColumnarExpression, name: String)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty,
    override val explicitMetadata: Option[Metadata] = None,
    override val nonInheritableMetadataKeys: Seq[String] = Seq.empty)
  extends Alias(child, name)(exprId, qualifier, explicitMetadata, nonInheritableMetadataKeys)
  with ColumnarExpression {

  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)

  override protected def withNewChildInternal(newChild: Expression): ColumnarAlias =
    new ColumnarAlias(newChild.asInstanceOf[ColumnarExpression], name)(exprId, qualifier,
      explicitMetadata, nonInheritableMetadataKeys)
}

class ColumnarAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty[String])
  extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
  with ColumnarExpression {

  // No columnar eval is needed because this must be bound before it is evaluated
}

class ColumnarLiteral (value: Any, dataType: DataType) extends Literal(value, dataType)
  with ColumnarExpression {
  override def columnarEval(batch: ColumnarBatch): Any = value
}

/**
 * A version of ProjectExec that adds in columnar support.
 */
class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExec(projectList, child) {

  override def supportsColumnar: Boolean =
    projectList.forall(_.asInstanceOf[ColumnarExpression].supportsColumnar)

  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] =
      ColumnarBindReferences.bindReferences(
        projectList.asInstanceOf[Seq[ColumnarExpression]], child.output)
    val rdd = child.executeColumnar()
    rdd.mapPartitions((itr) => CloseableColumnBatchIterator(itr,
      (cb) => {
        val newColumns = boundProjectList.map(
          expr => expr.asInstanceOf[ColumnarExpression].columnarEval(cb).asInstanceOf[ColumnVector]
        ).toArray
        new ColumnarBatch(newColumns, cb.numRows())
      })
    )
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarProjectExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarProjectExec =
    new ColumnarProjectExec(projectList, newChild)
}

/**
 * A version of add that supports columnar processing for longs.  This version is broken
 * on purpose so it adds the numbers plus 1 so that the tests can show that it was replaced.
 */
class BrokenColumnarAdd(
    left: ColumnarExpression,
    right: ColumnarExpression,
    failOnError: Boolean = false)
  extends Add(left, right, failOnError) with ColumnarExpression {

  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      if (lhs == null || rhs == null) {
        ret = null
      } else if (lhs.isInstanceOf[ColumnVector] && rhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[ColumnVector]
        val r = rhs.asInstanceOf[ColumnVector]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l.getLong(i) + r.getLong(i) + 1) // BUG to show we replaced Add
        }
      } else if (rhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[Long]
        val r = rhs.asInstanceOf[ColumnVector]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l + r.getLong(i) + 1) // BUG to show we replaced Add
        }
      } else if (lhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[ColumnVector]
        val r = rhs.asInstanceOf[Long]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l.getLong(i) + r + 1) // BUG to show we replaced Add
        }
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): BrokenColumnarAdd =
    new BrokenColumnarAdd(
      left = newLeft.asInstanceOf[ColumnarExpression],
      right = newRight.asInstanceOf[ColumnarExpression], failOnError)
}

class CannotReplaceException(str: String) extends RuntimeException(str) {

}

case class PreRuleReplaceAddWithBrokenVersion() extends Rule[SparkPlan] {
  def replaceWithColumnarExpression(exp: Expression): ColumnarExpression = exp match {
    case a: Alias =>
      new ColumnarAlias(replaceWithColumnarExpression(a.child),
        a.name)(a.exprId, a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
    case att: AttributeReference =>
      new ColumnarAttributeReference(att.name, att.dataType, att.nullable,
        att.metadata)(att.exprId, att.qualifier)
    case lit: Literal =>
      new ColumnarLiteral(lit.value, lit.dataType)
    case add: Add if (add.dataType == LongType) &&
      (add.left.dataType == LongType) &&
      (add.right.dataType == LongType) =>
      // Add only supports Longs for now.
      new BrokenColumnarAdd(replaceWithColumnarExpression(add.left),
        replaceWithColumnarExpression(add.right))
    case exp =>
      throw new CannotReplaceException(s"expression " +
        s"${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan =
    try {
      plan match {
        case e: ShuffleExchangeExec =>
          // note that this is not actually columnar but demonstrates that exchanges can
          // be replaced.
          val replaced = e.withNewChildren(e.children.map(replaceWithColumnarPlan))
          MyShuffleExchangeExec(replaced.asInstanceOf[ShuffleExchangeExec])
        case e: BroadcastExchangeExec =>
          // note that this is not actually columnar but demonstrates that exchanges can
          // be replaced.
          val replaced = e.withNewChildren(e.children.map(replaceWithColumnarPlan))
          MyBroadcastExchangeExec(replaced.asInstanceOf[BroadcastExchangeExec])
        case plan: ProjectExec =>
          new ColumnarProjectExec(plan.projectList.map((exp) =>
            replaceWithColumnarExpression(exp).asInstanceOf[NamedExpression]),
            replaceWithColumnarPlan(plan.child))
        case p =>
          logWarning(s"Columnar processing for ${p.getClass} is not currently supported.")
          p.withNewChildren(p.children.map(replaceWithColumnarPlan))
      }
    } catch {
      case exp: CannotReplaceException =>
        logWarning(s"Columnar processing for ${plan.getClass} is not currently supported" +
          s"because ${exp.getMessage}")
        plan
    }

  override def apply(plan: SparkPlan): SparkPlan = replaceWithColumnarPlan(plan)
}

/**
 * Custom Exchange used in tests to demonstrate that shuffles can be replaced regardless of
 * whether AQE is enabled.
 */
case class MyShuffleExchangeExec(delegate: ShuffleExchangeExec) extends ShuffleExchangeLike {
  override def numMappers: Int = delegate.numMappers
  override def numPartitions: Int = delegate.numPartitions
  override def shuffleOrigin: ShuffleOrigin = {
    delegate.shuffleOrigin
  }
  override def mapOutputStatisticsFuture: Future[MapOutputStatistics] =
    delegate.submitShuffleJob
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] =
    delegate.getShuffleRDD(partitionSpecs)
  override def runtimeStatistics: Statistics = delegate.runtimeStatistics
  override def child: SparkPlan = delegate.child
  override protected def doExecute(): RDD[InternalRow] = delegate.execute()
  override def outputPartitioning: Partitioning = delegate.outputPartitioning
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    super.legacyWithNewChildren(Seq(newChild))
}

/**
 * Custom Exchange used in tests to demonstrate that broadcasts can be replaced regardless of
 * whether AQE is enabled.
 */
case class MyBroadcastExchangeExec(delegate: BroadcastExchangeExec) extends BroadcastExchangeLike {
  override def runId: UUID = delegate.runId
  override def relationFuture: java.util.concurrent.Future[Broadcast[Any]] =
    delegate.relationFuture
  override def completionFuture: Future[Broadcast[Any]] = delegate.submitBroadcastJob
  override def runtimeStatistics: Statistics = delegate.runtimeStatistics
  override def child: SparkPlan = delegate.child
  override protected def doPrepare(): Unit = delegate.prepare()
  override protected def doExecute(): RDD[InternalRow] = delegate.execute()
  override def doExecuteBroadcast[T](): Broadcast[T] = delegate.executeBroadcast()
  override def outputPartitioning: Partitioning = delegate.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    super.legacyWithNewChildren(Seq(newChild))
}

class ReplacedRowToColumnarExec(override val child: SparkPlan)
  extends RowToColumnarExec(child) {

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ReplacedRowToColumnarExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ReplacedRowToColumnarExec =
    new ReplacedRowToColumnarExec(newChild)
}

case class MyPostRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case rc: RowToColumnarExec => new ReplacedRowToColumnarExec(rc.child)
    case plan => plan.withNewChildren(plan.children.map(apply))
  }
}

case class MyColumnarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan]) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre
  override def postColumnarTransitions: Rule[SparkPlan] = post
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy)
    e.injectResolutionRule(MyRule)
    e.injectPostHocResolutionRule(MyRule)
    e.injectCheckRule(MyCheckRule)
    e.injectOptimizerRule(MyRule)
    e.injectParser(MyParser)
    e.injectFunction(MyExtensions.myFunction)
    e.injectColumnar(session => MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
  }
}

object QueryPrepRuleHelper {
  val myPrepTag: TreeNodeTag[String] = TreeNodeTag[String]("myPrepTag")
  val myPrepTagValue: String = "myPrepTagValue"
}

// this rule will run during AQE query preparation and will write custom tags to each node
case class MyQueryStagePrepRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case plan =>
      plan.setTagValue(QueryPrepRuleHelper.myPrepTag, QueryPrepRuleHelper.myPrepTagValue)
      plan
  }
}

// this rule will run during AQE query stage optimization and will verify custom tags were
// already written during query preparation phase
case class MyNewQueryStageRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case plan if !plan.isInstanceOf[AdaptiveSparkPlanExec] =>
      assert(plan.getTagValue(QueryPrepRuleHelper.myPrepTag).get ==
          QueryPrepRuleHelper.myPrepTagValue)
      plan
  }
}

case class MyRule2(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule2(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy2(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

object MyExtensions2 {

  val myFunction = (FunctionIdentifier("myFunction2"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction2",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions2 extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy2)
    e.injectResolutionRule(MyRule2)
    e.injectPostHocResolutionRule(MyRule2)
    e.injectCheckRule(MyCheckRule2)
    e.injectOptimizerRule(MyRule2)
    e.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
    e.injectFunction(MyExtensions2.myFunction)
  }
}

object MyExtensions2Duplicate {

  val myFunction = (FunctionIdentifier("myFunction2"),
    new ExpressionInfo(
      "noClass",
      "myDb",
      "myFunction2",
      "usage",
      "extended usage",
      "    Examples:",
      """
       note
      """,
      "",
      "3.0.0",
      """
       deprecated
      """,
      ""),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions2Duplicate extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectFunction(MyExtensions2Duplicate.myFunction)
  }
}

class YourExtensions extends SparkSessionExtensionsProvider {
  val getAppName = (FunctionIdentifier("get_fake_app_name"),
    new ExpressionInfo(
      "zzz.zzz.zzz",
      "",
      "get_fake_app_name"),
    (_: Seq[Expression]) => Literal("Fake App Name"))

  override def apply(v1: SparkSessionExtensions): Unit = {
    v1.injectFunction(getAppName)
  }
}
