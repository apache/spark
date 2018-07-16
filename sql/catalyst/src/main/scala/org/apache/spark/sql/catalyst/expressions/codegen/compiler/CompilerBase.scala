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

package org.apache.spark.sql.catalyst.expressions.codegen.compiler

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.control.NonFatal

import org.codehaus.janino.util.ClassFile

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, GeneratedClass}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.Utils


abstract class CompilerBase extends Logging {
  protected val className = "org.apache.spark.sql.catalyst.expressions.GeneratedClass"

  protected val importClassNames = Seq(
    classOf[Platform].getName,
    classOf[InternalRow].getName,
    classOf[UnsafeRow].getName,
    classOf[UnsafeProjection].getName,
    classOf[UTF8String].getName,
    classOf[Decimal].getName,
    classOf[CalendarInterval].getName,
    classOf[ArrayData].getName,
    classOf[UnsafeArrayData].getName,
    classOf[MapData].getName,
    classOf[UnsafeMapData].getName,
    classOf[Expression].getName,
    classOf[TaskContext].getName,
    classOf[TaskKilledException].getName,
    classOf[InputMetrics].getName
  )

  protected val extendedClass = classOf[GeneratedClass]

  protected val debugSource = true
  protected val debugLines = true
  protected val debugVars = false

  protected def prefixLineNumbers(code: String): String = {
    if (!debugLines) {
      return code
    }
    val out = new StringBuilder(code.length * 3 / 2)
    var i = 1
    for (line <- code.split("\n")) {
      val start = out.length
      out.append(i)
      i += 1
      val numLength = out.length() - start
      out.append(":")
      for (spaces <- 0 until 7 - numLength) {
        out.append(" ")
      }
      out.append(line)
      out.append('\n')
    }
    out.toString()
  }

  def compile(code: CodeAndComment): (GeneratedClass, Int)

  /**
   * Returns the max bytecode size of the generated functions by inspecting janino private fields.
   * Also, this method updates the metrics information.
   */
  protected def updateAndGetBytecodeSize(byteCodes: Iterable[Array[Byte]]): Int = {
    // Walk the classes to get at the method bytecode.
    val codeAttr = Utils.classForName("org.codehaus.janino.util.ClassFile$CodeAttribute")
    val codeAttrField = codeAttr.getDeclaredField("code")
    codeAttrField.setAccessible(true)
    val codeSizes = byteCodes.flatMap { byteCode =>
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(byteCode.size)
      try {
        val cf = new ClassFile(new ByteArrayInputStream(byteCode))
        val stats = cf.methodInfos.asScala.flatMap { method =>
          method.getAttributes().filter(_.getClass.getName == codeAttr.getName).map { a =>
            val byteCodeSize = codeAttrField.get(a).asInstanceOf[Array[Byte]].length
            CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.update(byteCodeSize)
            byteCodeSize
          }
        }
        Some(stats)
      } catch {
        case NonFatal(e) =>
          logWarning("Error calculating stats of compiled class.", e)
          None
      }
    }.flatten

    codeSizes.max
  }
}
