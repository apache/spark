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

package org.apache.spark.sql.catalyst.analysis

import java.util.LinkedHashMap

import org.apache.spark.sql.catalyst.expressions._

object NondeterministicExpressionCollection {
  def getNondeterministicToAttributes(
      expressions: Seq[Expression]): LinkedHashMap[Expression, NamedExpression] = {
    val nonDeterministicToAttributes = new LinkedHashMap[Expression, NamedExpression]

    for (expr <- expressions) {
      if (!expr.deterministic) {
        val leafNondeterministic = expr.collect {
          case nondeterministicExpr: Nondeterministic => nondeterministicExpr
          case udf: UserDefinedExpression if !udf.deterministic => udf
          case udf: ExternalUserDefinedFunction if !udf.deterministic => udf
          // AesEncrypt draws an IV without extending Nondeterministic. That
          // trait would also mark ECB and an explicit IV nondeterministic.
          case aes: AesEncrypt if !aes.deterministic => aes
        }

        for (nondeterministicExpr <- leafNondeterministic.distinct) {
          expressionToPull(expr, nondeterministicExpr).foreach { toPull =>
            val key = toPull.canonicalized
            if (!nonDeterministicToAttributes.containsKey(key)) {
              val namedExpression = toPull match {
                case namedExpression: NamedExpression => namedExpression
                case _ => Alias(toPull, "_nondeterministic")()
              }
              nonDeterministicToAttributes.put(key, namedExpression)
            }
          }
        }
      }
    }

    nonDeterministicToAttributes
  }

  // A collected call that reads a lambda variable is bound only inside that
  // lambda. Projecting the call below the operator leaves the variable
  // unbound. Project the smallest ancestor that still binds it, such as the
  // transform(...) around aes_encrypt(x, key). A call that does not read the
  // variable is projected itself.
  private def expressionToPull(root: Expression, leaf: Expression): Option[Expression] = {
    val hidden = leaf.references -- root.references
    if (hidden.isEmpty) {
      Some(leaf)
    } else {
      smallestEvaluableHost(root, leaf, hidden)
    }
  }

  private def smallestEvaluableHost(
      root: Expression,
      leaf: Expression,
      hidden: AttributeSet): Option[Expression] = {
    def search(e: Expression): Option[Expression] = {
      if (!containsNode(e, leaf)) {
        None
      } else {
        val childHost = e.children.iterator.map(search).collectFirst { case Some(host) => host }
        childHost.orElse {
          if (canHost(e, leaf, hidden)) Some(e) else None
        }
      }
    }
    search(root)
  }

  private def canHost(e: Expression, leaf: Expression, hidden: AttributeSet): Boolean = {
    (e ne leaf) &&
      !e.isInstanceOf[LambdaFunction] &&
      !e.isInstanceOf[Unevaluable] &&
      e.references.intersect(hidden).isEmpty
  }

  private def containsNode(root: Expression, target: Expression): Boolean = {
    (root eq target) || root.children.exists(containsNode(_, target))
  }
}
