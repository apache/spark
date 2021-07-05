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

import java.util.IdentityHashMap

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

/**
 * This class helps subexpression elimination for interpreted evaluation
 * such as `InterpretedUnsafeProjection`. It maintains an evaluation cache.
 * This class wraps `ExpressionProxy` around given expressions. The `ExpressionProxy`
 * intercepts expression evaluation and loads from the cache first.
 */
class SubExprEvaluationRuntime(cacheMaxEntries: Int) {
  // The id assigned to `ExpressionProxy`. `SubExprEvaluationRuntime` will use assigned ids of
  // `ExpressionProxy` to decide the equality when loading from cache. `SubExprEvaluationRuntime`
  // won't be use by multi-threads so we don't need to consider concurrency here.
  private var proxyExpressionCurrentId = 0

  private[sql] val cache: LoadingCache[ExpressionProxy, ResultProxy] =
    Caffeine.newBuilder().maximumSize(cacheMaxEntries)
      // SPARK-34309: Use custom Executor to compatible with
      // the data eviction behavior of Guava cache
      .executor((command: Runnable) => command.run())
      .build[ExpressionProxy, ResultProxy](
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
      equivalentExpressions: EquivalentExpressions,
      proxyMap: IdentityHashMap[Expression, ExpressionProxy]): Expression = {
    equivalentExpressions.getExprState(expr) match {
      case Some(stats) if proxyMap.containsKey(stats.expr) => proxyMap.get(stats.expr)
      case _ => expr.mapChildren(replaceWithProxy(_, equivalentExpressions, proxyMap))
    }
  }

  /**
   * Finds subexpressions and wraps them with `ExpressionProxy`.
   */
  def proxyExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

    expressions.foreach(equivalentExpressions.addExprTree(_))

    val proxyMap = new IdentityHashMap[Expression, ExpressionProxy]

    val commonExprs = equivalentExpressions.getCommonSubexpressions
    commonExprs.foreach { expr =>
      val proxy = ExpressionProxy(expr, proxyExpressionCurrentId, this)
      proxyExpressionCurrentId += 1

      // We leverage `IdentityHashMap` so we compare expression keys by reference here.
      // So for example if there are one group of common exprs like Seq(common expr 1,
      // common expr2, ..., common expr n), we will insert into `proxyMap` some key/value
      // pairs like Map(common expr 1 -> proxy(common expr 1), ...,
      // common expr n -> proxy(common expr 1)).
      proxyMap.put(expr, proxy)
    }

    // Only adding proxy if we find subexpressions.
    if (!proxyMap.isEmpty) {
      expressions.map(replaceWithProxy(_, equivalentExpressions, proxyMap))
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
    id: Int,
    runtime: SubExprEvaluationRuntime) extends UnaryExpression {

  final override def dataType: DataType = child.dataType
  final override def nullable: Boolean = child.nullable

  // `ExpressionProxy` is for interpreted expression evaluation only. So cannot `doGenCode`.
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  def proxyEval(input: InternalRow = null): Any = child.eval(input)

  override def eval(input: InternalRow = null): Any = runtime.getEval(this)

  override def equals(obj: Any): Boolean = obj match {
    case other: ExpressionProxy => this.id == other.id
    case _ => false
  }

  override def hashCode(): Int = this.id.hashCode()

  override protected def withNewChildInternal(newChild: Expression): ExpressionProxy =
    copy(child = newChild)
}

/**
 * A simple wrapper for holding `Any` in the cache of `SubExprEvaluationRuntime`.
 */
case class ResultProxy(result: Any)
