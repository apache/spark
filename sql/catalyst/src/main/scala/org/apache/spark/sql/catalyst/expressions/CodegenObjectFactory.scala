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

/**
 * Catches compile error during code generation.
 */
object CodegenError {
  def unapply(throwable: Throwable): Option[Exception] = throwable match {
    case e: InternalCompilerException => Some(e)
    case e: CompileException => Some(e)
    case _ => None
  }
}

/**
 * A factory class which can be used to create objects that have both codegen and interpreted
 * implementations. This tries to create codegen object first, if any compile error happens,
 * it fallbacks to interpreted version.
 */
abstract class CodegenObjectFactory[IN, OUT] {

  def createObject(in: IN): OUT = try {
    createCodeGeneratedObject(in)
  } catch {
    case CodegenError(_) => createInterpretedObject(in)
  }

  protected def createCodeGeneratedObject(in: IN): OUT
  protected def createInterpretedObject(in: IN): OUT
}

object UnsafeProjectionFactory extends CodegenObjectFactory[Seq[Expression], UnsafeProjection]
    with UnsafeProjectionCreator {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.createProjection(in)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): UnsafeProjection = {
    InterpretedUnsafeProjection.createProjection(in)
  }

  override protected[sql] def createProjection(exprs: Seq[Expression]): UnsafeProjection =
    createObject(exprs)

  /**
   * Same as other create()'s but allowing enabling/disabling subexpression elimination.
   * The param `subexpressionEliminationEnabled` doesn't guarantee to work. For example,
   * when fallbacking to interpreted execution, it is not supported.
   */
  def create(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = try {
    UnsafeProjection.create(exprs, inputSchema, subexpressionEliminationEnabled)
  } catch {
    case CodegenError(_) => InterpretedUnsafeProjection.create(exprs, inputSchema)
  }
}

