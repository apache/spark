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

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf

/**
 * This class helps subexpression elimination for interpreted evaluation
 * in `InterpretedUnsafeProjection`. It maintains an evaluation cache.
 * This class wraps `ExpressionProxy` around given expressions. The `ExpressionProxy`
 * intercepts expression evaluation and loads from the cache first.
 */
class EvaluationRunTime {

  val cache: LoadingCache[ExpressionProxy, ResultProxy] = CacheBuilder.newBuilder()
    .maximumSize(SQLConf.get.subexpressionEliminationCacheMaxEntries)
    .build(
      new CacheLoader[ExpressionProxy, ResultProxy]() {
        override def load(expr: ExpressionProxy): ResultProxy = {
          ResultProxy(expr.proxyEval(currentInput))
        }
      })

  private var currentInput: InternalRow = null

  /**
   * Sets given input row as current row for evaluating expressions. This cleans up the cache
   * too as new input comes.
   */
  def setInput(input: InternalRow = null): Unit = {
    currentInput = input
    cache.invalidateAll()
  }

  /**
   * Finds subexpressions and wraps them with `ExpressionProxy`.
   */
  def proxyExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

    expressions.foreach(equivalentExpressions.addExprTree(_))

    var proxyMap = Map.empty[Expression, ExpressionProxy]

    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    commonExprs.foreach { e =>
      val expr = e.head
      val proxy = ExpressionProxy(expr, this)

      proxyMap ++= e.map(_ -> proxy).toMap
    }

    // Only adding proxy if we find subexpressions.
    if (proxyMap.nonEmpty) {
      expressions.map { expr =>
        // `transform` will cause stackoverflow because it keeps transforming into
        // `ExpressionProxy`. But we cannot use `transformUp` because we want to use
        // subexpressions at higher level. So we `transformDown` until finding first
        // subexpression.
        var transformed = false
        expr.transform {
          case e if !transformed && proxyMap.contains(e) =>
            transformed = true
            proxyMap(e)
        }
      }
    } else {
      expressions
    }
  }
}
