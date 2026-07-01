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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.catalyst.expressions.CursorDefinition

/**
 * Trait which provides an interface extension for SQL scripting execution context.
 * Provides APIs for cursor lookup that can be used during analysis without creating
 * circular dependencies between catalyst and core modules.
 */
trait SqlScriptingExecutionContextExtension {

  /**
   * Find a cursor by its normalized name in the current scope and parent scopes.
   * Used for unqualified cursor references (e.g., `cursor`).
   *
   * @param normalizedName The normalized cursor name (considering case sensitivity)
   * @return The cursor definition if found
   */
  def findCursorByName(normalizedName: String): Option[CursorDefinition]

  /**
   * Find a cursor in a specific labeled scope.
   * Used for qualified cursor references (e.g., `label.cursor`).
   *
   * @param normalizedScopeLabel The normalized label of the scope to search in
   * @param normalizedName The normalized cursor name (considering case sensitivity)
   * @return The cursor definition if found
   */
  def findCursorInScope(
      normalizedScopeLabel: String,
      normalizedName: String): Option[CursorDefinition]
}
