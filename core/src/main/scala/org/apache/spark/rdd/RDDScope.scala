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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.SparkContext

/**
 * A collection of utility methods to construct a hierarchical representation of RDD scopes.
 * An RDD scope tracks the series of operations that created a given RDD.
 */
private[spark] object RDDScope {

  // Symbol for delimiting each level of the hierarchy
  // e.g. grandparent;parent;child
  val SCOPE_NESTING_DELIMITER = ";"

  // Symbol for delimiting the scope name from the ID within each level
  val SCOPE_NAME_DELIMITER = "_"

  // Counter for generating scope IDs, for differentiating
  // between different scopes of the same name
  private val scopeCounter = new AtomicInteger(0)

  /**
   * Make a globally unique scope ID from the scope name.
   *
   * For instance:
   *   textFile -> textFile_0
   *   textFile -> textFile_1
   *   map -> map_2
   *   name;with_sensitive;characters -> name-with-sensitive-characters_3
   */
  private def makeScopeId(name: String): String = {
    name.replace(SCOPE_NESTING_DELIMITER, "-").replace(SCOPE_NAME_DELIMITER, "-") +
      SCOPE_NAME_DELIMITER + scopeCounter.getAndIncrement
  }

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   * The name of the scope will be the name of the method that immediately encloses this one.
   *
   * Note: Return statements are NOT allowed in body.
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      allowNesting: Boolean = false)(body: => T): T = {
    val callerMethodName = Thread.currentThread.getStackTrace()(3).getMethodName
    withScope[T](sc, callerMethodName, allowNesting)(body)
  }

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   *
   * If nesting is allowed, this concatenates the previous scope with the new one in a way that
   * signifies the hierarchy. Otherwise, if nesting is not allowed, then any children calls to
   * this method executed in the body will have no effect.
   *
   * Note: Return statements are NOT allowed in body.
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      name: String,
      allowNesting: Boolean = false)(body: => T): T = {
    // Save the old scope to restore it later
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScope = sc.getLocalProperty(scopeKey)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
      // Set the scope only if the higher level caller allows us to do so
      if (sc.getLocalProperty(noOverrideKey) == null) {
        val oldScopeId = Option(oldScope).map { _ + SCOPE_NESTING_DELIMITER }.getOrElse("")
        val newScopeId = oldScopeId + makeScopeId(name)
        sc.setLocalProperty(scopeKey, newScopeId)
      }
      // Optionally disallow the child body to override our scope
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      sc.setLocalProperty(scopeKey, oldScope)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
