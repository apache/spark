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

import org.apache.spark.sql.internal.SQLConf

// A helper class to evaluate expressions.
trait ExpressionsEvaluator {
  protected lazy val runtime =
    new SubExprEvaluationRuntime(SQLConf.get.subexpressionEliminationCacheMaxEntries)

  protected def prepareExpressions(
      exprs: Seq[Expression],
      subExprEliminationEnabled: Boolean): Seq[Expression] = {
    // We need to make sure that we do not reuse stateful expressions.
    val cleanedExpressions = exprs.map(_.freshCopyIfContainsStatefulExpression())
    if (subExprEliminationEnabled) {
      runtime.proxyExpressions(cleanedExpressions)
    } else {
      cleanedExpressions
    }
  }

  /**
   * Initializes internal states given the current partition index.
   * This is used by nondeterministic expressions to set initial states.
   * The default implementation does nothing.
   */
  def initialize(partitionIndex: Int): Unit = {}

  protected def initializeExprs(exprs: Seq[Expression], partitionIndex: Int): Unit = {
    exprs.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }
}
