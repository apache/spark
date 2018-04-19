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

import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

object CodegenObjectFactory {
  def codegenOrInterpreted[T](codegenCreator: () => T, interpretedCreator: () => T): T = {
    try {
      codegenCreator()
    } catch {
      // Catch compile error related exceptions
      case e: InternalCompilerException => interpretedCreator()
      case e: CompileException => interpretedCreator()
    }
  }
}

object UnsafeProjectionFactory extends UnsafeProjectionCreator {
  import CodegenObjectFactory._

  private val codegenCreator = UnsafeProjection
  private lazy val interpretedCreator = InterpretedUnsafeProjection

  /**
   * Returns an [[UnsafeProjection]] for given sequence of bound Expressions.
   */
  override protected[sql] def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    codegenOrInterpreted[UnsafeProjection](() => codegenCreator.createProjection(exprs),
      () => interpretedCreator.createProjection(exprs))
  }

  /**
   * Same as other create()'s but allowing enabling/disabling subexpression elimination.
   * The param `subexpressionEliminationEnabled` doesn't guarantee to work. For example,
   * when fallbacking to interpreted execution, it is not supported.
   */
  def create(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    codegenOrInterpreted[UnsafeProjection](
      () => codegenCreator.create(exprs, inputSchema, subexpressionEliminationEnabled),
      () => interpretedCreator.create(exprs, inputSchema))
  }
}

