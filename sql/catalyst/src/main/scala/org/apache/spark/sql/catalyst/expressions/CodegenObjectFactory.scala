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

trait CodegenObjectFactoryBase[IN, OUT] {
  protected def createObject(in: IN): OUT
  protected def createCodeGeneratedObject(in: IN): OUT
  protected def createInterpretedObject(in: IN): OUT
}

/**
 * A factory which can be used to create objects that have both codegen and interpreted
 * implementations. This tries to create codegen object first, if any compile error happens,
 * it fallbacks to interpreted version.
 */
trait CodegenObjectFactory[IN, OUT] extends CodegenObjectFactoryBase[IN, OUT] {
  override protected def createObject(in: IN): OUT = try {
    createCodeGeneratedObject(in)
  } catch {
    case CodegenError(_) => createInterpretedObject(in)
  }
}

/**
 * A factory which can be used to create codegen objects without fallback to interpreted version.
 */
trait CodegenObjectFactoryWithoutFallback[IN, OUT] extends CodegenObjectFactoryBase[IN, OUT] {
  override protected def createObject(in: IN): OUT =
    createCodeGeneratedObject(in)
}

/**
 * A factory which can be used to create objects with interpreted implementation.
 */
trait InterpretedCodegenObjectFactory[IN, OUT] extends CodegenObjectFactoryBase[IN, OUT] {
  override protected def createObject(in: IN): OUT =
    createInterpretedObject(in)
}
