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

package org.apache.spark.sql.execution

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, CreateNamedStruct, Expression, ExprId, In, InBloomFilter, InSet, ListQuery, Literal, PlanExpression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.BloomFilterUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, BooleanType, DataType, StructType}
import org.apache.spark.util.sketch.BloomFilter

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[BaseSubqueryExec] {
  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit

  /** Updates the expression with a new plan. */
  override def withNewPlan(plan: BaseSubqueryExec): ExecSubqueryExpression
}

object ExecSubqueryExpression {
  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.find {
      case _: ExecSubqueryExpression => true
      case _ => false
    }.isDefined
  }
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): ScalarSubquery = copy(plan = query)

  override def semanticEquals(other: Expression): Boolean = other match {
    case s: ScalarSubquery => plan.sameResult(s.plan)
    case _ => false
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      sys.error(s"more than one row returned by a subquery used as an expression:\n$plan")
    }
    if (rows.length == 1) {
      assert(rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      result = rows(0).get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}

/**
 * The physical node of in-subquery. This is for Dynamic Partition Pruning only, as in-subquery
 * coming from the original query will always be converted to joins.
 */
case class InSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    private var resultBroadcast: Broadcast[Set[Any]] = null) extends ExecSubqueryExpression {

  @transient private var result: Set[Any] = _
  @transient private lazy val inSet = InSet(child, result)

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = child.nullable
  override def toString: String = s"$child IN ${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): InSubqueryExec = copy(plan = plan)

  override def semanticEquals(other: Expression): Boolean = other match {
    case in: InSubqueryExec => child.semanticEquals(in.child) && plan.sameResult(in.plan)
    case _ => false
  }

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = if (plan.output.length > 1) {
      rows.toSet
    } else {
      rows.map(_.get(0, child.dataType)).toSet
    }
    resultBroadcast = plan.sqlContext.sparkContext.broadcast(result)
  }

  def values(): Option[Set[Any]] = Option(resultBroadcast).map(_.value)

  lazy val pushedFilter: Option[Predicate] = {
    prepareResult()
    child.dataType match {
      case _: AtomicType if result.size <= SQLConf.get.parquetFilterPushDownInFilterThreshold =>
        val converter = CatalystTypeConverters.createToScalaConverter(child.dataType)
        // scalastyle:off
        println(result.map(v => Literal.create(converter(v), child.dataType)).toSeq)
        println("set result" + result.toSeq.sortBy(_.toString.toInt))
        Some(In(child, result.map(v => Literal.create(converter(v), child.dataType)).toSeq))
      case _ => None
    }
  }

  private def prepareResult(): Unit = {
    require(resultBroadcast != null, s"$this has not finished")
    if (result == null) {
      result = resultBroadcast.value
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    inSet.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()
    inSet.doGenCode(ctx, ev)
  }

  override lazy val canonicalized: InSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null)
  }
}

case class BloomFilterWithSmallSet(bloomFilter: BloomFilter, set: Option[Set[Any]])

case class BloomFilterSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    private var resultBroadcast: Broadcast[BloomFilterWithSmallSet] = null)
  extends ExecSubqueryExpression {

  @transient private var result: BloomFilterWithSmallSet = _
  @transient private lazy val inBloomFilter = InBloomFilter(child, result.bloomFilter)

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = child.nullable
  override def toString: String = s"$child IN BLOOM FILTER ${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): BloomFilterSubqueryExec = copy(plan = plan)

  override def semanticEquals(other: Expression): Boolean = other match {
    case bf: BloomFilterSubqueryExec => child.semanticEquals(bf.child) && plan.sameResult(bf.plan)
    case _ => false
  }

  def updateResult(): Unit = {
    val rows = plan.executeCollect().map(_.get(0, child.dataType))
    val numElements = plan.conf.getConfString("spark.sql.bloomFilterElements", "10000").toLong
    println(s"numElements: ${numElements}")
    val expectedNumItems = math.max(rows.length, numElements)
    val bf = BloomFilter.create(expectedNumItems)
    rows.foreach(r => BloomFilterUtils.putValue(bf, r))
    child.dataType match {
      case _: AtomicType if rows.nonEmpty &&
        rows.length <= SQLConf.get.parquetFilterPushDownInFilterThreshold =>
        result = BloomFilterWithSmallSet(bf, Some(rows.toSet))
      case _ =>
        result = BloomFilterWithSmallSet(bf, None)
    }
    resultBroadcast = plan.sqlContext.sparkContext.broadcast(result)
  }

  def values(): Option[BloomFilterWithSmallSet] = Option(resultBroadcast).map(_.value)

  lazy val pushedFilter: Option[Predicate] = {
    prepareResult()
    result match {
      case BloomFilterWithSmallSet(_, Some(set)) =>
        val converter = CatalystTypeConverters.createToScalaConverter(child.dataType)
        // val converter = CatalystTypeConverters.createToCatalystConverter(child.dataType)
        // scalastyle:off
        println(set.map(v => Literal.create(converter(v), child.dataType)).toSeq)
        println("set result" + set.toSeq.sortBy(_.toString.toInt))
        Some(In(child, set.map(v => Literal.create(converter(v), child.dataType)).toSeq))
      case _ => None
    }
  }

  private def prepareResult(): Unit = {
    require(resultBroadcast != null, s"$this has not finished")
    if (result == null) {
      result = resultBroadcast.value
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    if (pushedFilter.nonEmpty) {
      true
    } else {
      inBloomFilter.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()
    if (pushedFilter.nonEmpty) {
      Literal.create(true, BooleanType).doGenCode(ctx, ev)
    } else {
      inBloomFilter.doGenCode(ctx, ev)
    }
  }

  override lazy val canonicalized: BloomFilterSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null)
  }
}

/**
 * Plans subqueries that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, subquery.plan)
        ScalarSubquery(
          SubqueryExec(s"scalar-subquery#${subquery.exprId.id}", executedPlan),
          subquery.exprId)
      case expressions.InSubquery(values, ListQuery(query, _, exprId, _)) =>
        val expr = if (values.length == 1) {
          values.head
        } else {
          CreateNamedStruct(
            values.zipWithIndex.flatMap { case (v, index) =>
              Seq(Literal(s"col_$index"), v)
            }
          )
        }
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, query)
        InSubqueryExec(expr, SubqueryExec(s"subquery#${exprId.id}", executedPlan), exprId)
    }
  }
}

/**
 * Find out duplicated subqueries in the spark plan, then use the same subquery result for all the
 * references.
 */
case class ReuseSubquery(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.subqueryReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of subqueries to avoid O(N*N) sameResult calls.
    val subqueries = mutable.HashMap[StructType, ArrayBuffer[BaseSubqueryExec]]()
    plan transformAllExpressions {
      case sub: ExecSubqueryExpression =>
        val sameSchema =
          subqueries.getOrElseUpdate(sub.plan.schema, ArrayBuffer[BaseSubqueryExec]())
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          sub.withNewPlan(ReusedSubqueryExec(sameResult.get))
        } else {
          sameSchema += sub.plan
          sub
        }
    }
  }
}
