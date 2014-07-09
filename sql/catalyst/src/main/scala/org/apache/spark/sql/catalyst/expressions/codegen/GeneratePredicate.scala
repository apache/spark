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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions._

/**
 * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[Row]].
 */
object GeneratePredicate extends CodeGenerator {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  // TODO: Should be weak references... bounded in size.
  val predicateCache = new collection.mutable.HashMap[Expression, (Row) => Boolean]

  // TODO: Safe to fire up multiple instances of the compiler?
  def apply(predicate: Expression): (Row => Boolean) = CodeGeneration.synchronized {
    val cleanedExpression = ExpressionCanonicalizer(predicate)
    predicateCache.getOrElseUpdate(cleanedExpression, createPredicate(cleanedExpression))
  }

  def apply(predicate: Expression, inputSchema: Seq[Attribute]): (Row => Boolean) =
    apply(BindReferences.bindReference(predicate, inputSchema))

  def createPredicate(predicate: Expression): ((Row) => Boolean) = {
    val cEval = expressionEvaluator(predicate)

    val code =
      q"""
        (i: $rowType) => {
          ..${cEval.code}
          if (${cEval.nullTerm}) false else ${cEval.primitiveTerm}
        }
      """

    log.debug(s"Generated predicate '$predicate':\n$code")
    toolBox.eval(code).asInstanceOf[Row => Boolean]
  }
}