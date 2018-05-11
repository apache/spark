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

import org.apache.spark.TaskContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

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
 * Defines values for `SQLConf` config of fallback mode. Use for test only.
 */
object CodegenObjectFactoryMode extends Enumeration {
  val AUTO, CODEGEN_ONLY, NO_CODEGEN = Value

  def currentMode: CodegenObjectFactoryMode.Value = {
    // If we weren't on task execution, accesses that config.
    if (TaskContext.get == null) {
      val config = SQLConf.get.getConf(SQLConf.CODEGEN_FACTORY_MODE)
      CodegenObjectFactoryMode.withName(config)
    } else {
      CodegenObjectFactoryMode.AUTO
    }
  }
}

/**
 * A factory which can be used to create objects that have both codegen and interpreted
 * implementations. This tries to create codegen object first, if any compile error happens,
 * it fallbacks to interpreted version.
 */
abstract class CodegenObjectFactory[IN, OUT] {

  def createObject(in: IN): OUT = {
    // We are allowed to choose codegen-only or no-codegen modes if under tests.
    if (Utils.isTesting && CodegenObjectFactoryMode.currentMode != CodegenObjectFactoryMode.AUTO) {
      CodegenObjectFactoryMode.currentMode match {
        case CodegenObjectFactoryMode.CODEGEN_ONLY => createCodeGeneratedObject(in)
        case CodegenObjectFactoryMode.NO_CODEGEN => createInterpretedObject(in)
      }
    } else {
      try {
        createCodeGeneratedObject(in)
      } catch {
        case CodegenError(_) => createInterpretedObject(in)
      }
    }
  }

  protected def createCodeGeneratedObject(in: IN): OUT
  protected def createInterpretedObject(in: IN): OUT
}
