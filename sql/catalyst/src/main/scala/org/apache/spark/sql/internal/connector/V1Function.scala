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

package org.apache.spark.sql.internal.connector

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.StructType

/**
 * A wrapper for V1 scalar functions that mirrors the V2 UnboundFunction pattern.
 *
 * V1Function has two responsibilities:
 * 1. Provide ExpressionInfo for DESCRIBE FUNCTION (accessed via `info`)
 * 2. Provide a function builder for invocation (accessed via `invoke()`)
 *
 * The key design is two-phase lazy loading:
 * - `info` is computed eagerly or from cache (no resource loading)
 * - `functionBuilder` is computed lazily on first `invoke()` (triggers resource loading)
 *
 * This matches V1 behavior where DESCRIBE doesn't load resources but invocation does.
 */
class V1Function private (
    val info: ExpressionInfo,
    builderFactory: () => FunctionBuilder) extends UnboundFunction {

  /**
   * Lazy function builder - only computed on first invoke().
   * For persistent functions, this triggers resource loading.
   */
  private lazy val functionBuilder: FunctionBuilder = builderFactory()

  /**
   * Invoke the function with the given arguments.
   * This is the V1 equivalent of V2's bind() pattern.
   * For persistent functions, first invocation triggers resource loading.
   */
  def invoke(arguments: Seq[Expression]): Expression = functionBuilder(arguments)

  // UnboundFunction interface
  override def bind(inputType: StructType): BoundFunction = {
    // V1 functions don't use the V2 bind() pattern - they use invoke() instead
    throw SparkException.internalError("V1Function.bind() should not be called")
  }

  override def name(): String = info.getName

  override def description(): String = info.getUsage
}

object V1Function {
  /** A placeholder builder for metadata-only V1Functions that throws on invocation. */
  private val metadataOnlyBuilder: FunctionBuilder = _ =>
    throw SparkException.internalError(
      "Metadata-only V1Function should not be invoked")

  /**
   * Create a V1Function with eager info and lazy builder.
   * The builderFactory is called on first invoke().
   */
  def apply(info: ExpressionInfo, builderFactory: () => FunctionBuilder): V1Function = {
    new V1Function(info, builderFactory)
  }

  /**
   * Create a V1Function with eager info and builder (for built-in/temp/cached functions).
   */
  def apply(info: ExpressionInfo, builder: FunctionBuilder): V1Function = {
    new V1Function(info, () => builder)
  }

  /**
   * Create a metadata-only V1Function (for DESCRIBE FUNCTION).
   * If invoke() is called, it will throw an error.
   */
  def metadataOnly(info: ExpressionInfo): V1Function = {
    new V1Function(info, () => metadataOnlyBuilder)
  }
}
