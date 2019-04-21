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

package org.apache.spark.sql.catalyst.bytecode

import org.apache.spark.sql.catalyst.bytecode.InstructionHandler.{Complete, Continue, Jump}
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * The object that converts JVM [[Behavior]]s into Catalyst expressions.
 */
private[bytecode] object BehaviorConverter {

  def convert(behavior: Behavior, localVars: LocalVarArray): Option[Expression] = {
    if (SpecialInstanceMethodHandler.canHandle(behavior)) {
      SpecialInstanceMethodHandler.handle(behavior, localVars)
    } else if (SpecialStaticMethodHandler.canHandle(behavior)) {
      SpecialStaticMethodHandler.handle(behavior, localVars)
    } else {
      convert(0, behavior, new OperandStack(), localVars)
    }
  }

  def convert(
      index: Int,
      behavior: Behavior,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Expression] = {

    val codeIter = behavior.newCodeIterator()
    codeIter.move(index)
    while (codeIter.hasNext) {
      val index = codeIter.next()
      val opcode = codeIter.byteAt(index)
      val instruction = Instruction(opcode, index, behavior)
      InstructionHandler.handle(instruction, operandStack, localVars) match {
        case Complete(returnValue) =>
          return returnValue
        case Jump(targetIndex) =>
          codeIter.move(targetIndex)
        case Continue =>
          // continue the conversion
      }
    }
    throw new RuntimeException("Could not complete the conversion: no instructions left")
  }
}
