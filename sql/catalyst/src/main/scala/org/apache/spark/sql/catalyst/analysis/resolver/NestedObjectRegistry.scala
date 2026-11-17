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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.ArrayDeque

import org.apache.spark.sql.catalyst.SQLConfHelper

/**
 * Resolution state owned by the current nested object.
 *
 * @param defaultCollation The object's default collation. A nested object's [[None]] replaces,
 *   rather than inherits, the enclosing object's default collation. Ordinary subqueries do not
 *   create a scope and therefore inherit this value.
 * @param referredTempVariableNames The temporary variables captured by the object. [[None]] is
 *   reserved for the root scope, where temporary variable resolution is unrestricted.
 */
case class NestedObjectScope(
    defaultCollation: Option[String],
    referredTempVariableNames: Option[Seq[Seq[String]]])

/**
 * Maintains the stack of state active while nested objects are resolved.
 *
 * The permanent root scope represents the context where query resolution started. Each nested
 * object pushes its own scope and restores the enclosing scope when its body has been resolved.
 */
class NestedObjectRegistry extends SQLConfHelper {
  private val rootScope = NestedObjectScope(
    defaultCollation = getRootDefaultCollation,
    referredTempVariableNames = None
  )
  private val stack = new ArrayDeque[NestedObjectScope]

  /** Returns the innermost nested-object scope, or the permanent root scope. */
  def current: NestedObjectScope = Option(stack.peek()).getOrElse(rootScope)

  /** Pushes a new nested-object scope. */
  def pushScope(
      defaultCollation: Option[String],
      referredTempVariableNames: Seq[Seq[String]]): Unit = {
    stack.push(
      NestedObjectScope(
        defaultCollation = defaultCollation,
        referredTempVariableNames = Some(referredTempVariableNames)
      )
    )
  }

  /** Pops the current nested-object scope. */
  def popScope(): Unit = {
    stack.pop()
  }

  /**
   * Returns the default collation of the root scope. Query resolution that doesn't start inside a
   * nested object uses no object default collation.
   */
  private def getRootDefaultCollation: Option[String] = None
}
