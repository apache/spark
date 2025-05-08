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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

/**
 * Defines values for `SQLConf` config of fallback mode. Use for test only.
 */
object CodegenObjectFactoryMode extends Enumeration {
  val FALLBACK, CODEGEN_ONLY, NO_CODEGEN = Value
}

/**
 * A codegen object generator which creates objects with codegen path first. Once any compile
 * error happens, it can fallback to interpreted implementation. In tests, we can use a SQL config
 * `SQLConf.CODEGEN_FACTORY_MODE` to control fallback behavior.
 */
abstract class CodeGeneratorWithInterpretedFallback[IN, OUT] extends Logging {

  def createObject(in: IN): OUT = {
    // We are allowed to choose codegen-only or no-codegen modes if under tests.
    val fallbackMode = SQLConf.get.codegenFactoryMode

    fallbackMode match {
      case CodegenObjectFactoryMode.CODEGEN_ONLY =>
        createCodeGeneratedObject(in)
      case CodegenObjectFactoryMode.NO_CODEGEN =>
        createInterpretedObject(in)
      case _ =>
        try {
          createCodeGeneratedObject(in)
        } catch {
          case NonFatal(e) =>
            logWarning("Expr codegen error and falling back to interpreter mode", e)
            createInterpretedObject(in)
        }
    }
  }

  protected def createCodeGeneratedObject(in: IN): OUT
  protected def createInterpretedObject(in: IN): OUT
}
