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

import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

/**
 * This class helps subexpression elimination for interpreted evaluation
 * such as `InterpretedUnsafeProjection`. It maintains an evaluation cache.
 * This class wraps `ExpressionProxy` around given expressions. The `ExpressionProxy`
 * intercepts expression evaluation and loads from the cache first.
 */
class SubExprEvaluationRuntime(cacheMaxEntries: Int) {

  private[sql] val cache: LoadingCache[ExpressionProxy, ResultProxy] = CacheBuilder.newBuilder()
    .maximumSize(cacheMaxEntries)
    .build(
      new CacheLoader[ExpressionProxy, ResultProxy]() {
        override def load(expr: ExpressionProxy): ResultProxy = {
          ResultProxy(expr.proxyEval(currentInput))
        }
      })

  private var currentInput: InternalRow = null

  def getEval(proxy: ExpressionProxy): Any = try {
    cache.get(proxy).result
  } catch {
    // Cache.get() may wrap the original exception. See the following URL
    // http://google.github.io/guava/releases/14.0/api/docs/com/google/common/cache/
    //   Cache.html#get(K,%20java.util.concurrent.Callable)
    case e @ (_: UncheckedExecutionException | _: ExecutionError) =>
      throw e.getCause
  }

  /**
   * Sets given input row as current row for evaluating expressions. This cleans up the cache
   * too as new input comes.
   */
  def setInput(input: InternalRow = null): Unit = {
    currentInput = input
    cache.invalidateAll()
  }

  /**
   * Recursively replaces expression with its proxy expression in `proxyMap`.
   */
  private def replaceWithProxy(
      expr: Expression,
      proxyMap: Map[Expression, ExpressionProxy]): Expression = {
    proxyMap.getOrElse(expr, expr.mapChildren(replaceWithProxy(_, proxyMap)))
  }

  /**
   * Finds subexpressions and wraps them with `ExpressionProxy`.
   */
  def proxyExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

    expressions.foreach(equivalentExpressions.addExprTree(_))

    val proxyMap = mutable.Map.empty[Expression, ExpressionProxy]

    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    commonExprs.foreach { e =>
      val expr = e.head
      val proxy = ExpressionProxy(expr, this)

      proxyMap ++= e.map(_ -> proxy).toMap
    }

    // Only adding proxy if we find subexpressions.
    if (proxyMap.nonEmpty) {
      expressions.map(replaceWithProxy(_, proxyMap.toMap))
    } else {
      expressions
    }
  }
}

/**
 * A proxy for an catalyst `Expression`. Given a runtime object `SubExprEvaluationRuntime`,
 * when this is asked to evaluate, it will load from the evaluation cache in the runtime first.
 */
case class ExpressionProxy(
    child: Expression,
    runtime: SubExprEvaluationRuntime) extends Expression {

  final override def dataType: DataType = child.dataType
  final override def nullable: Boolean = child.nullable
  final override def children: Seq[Expression] = child :: Nil

  // `ExpressionProxy` is for interpreted expression evaluation only. So cannot `doGenCode`.
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")

  def proxyEval(input: InternalRow = null): Any = child.eval(input)

  override def eval(input: InternalRow = null): Any = runtime.getEval(this)
}

/**
 * A simple wrapper for holding `Any` in the cache of `SubExprEvaluationRuntime`.
 */
case class ResultProxy(result: Any)
