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

import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._

import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino._

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{ParentClassLoader, Utils}


object JaninoCompiler extends CompilerBase {

  private def updateAndGetCompilationStats(evaluator: ClassBodyEvaluator): Int = {
    // First retrieve the generated classes.
    val classes = {
      val resultField = classOf[SimpleCompiler].getDeclaredField("result")
      resultField.setAccessible(true)
      val loader = resultField.get(evaluator).asInstanceOf[ByteArrayClassLoader]
      val classesField = loader.getClass.getDeclaredField("classes")
      classesField.setAccessible(true)
      classesField.get(loader).asInstanceOf[JavaMap[String, Array[Byte]]].asScala
    }
    updateAndGetBytecodeSize(classes.values)
  }

  override def compile(code: CodeAndComment): (GeneratedClass, Int) = {
    val evaluator = new ClassBodyEvaluator()

    // A special classloader used to wrap the actual parent classloader of
    // [[org.codehaus.janino.ClassBodyEvaluator]] (see CodeGenerator.doCompile). This classloader
    // does not throw a ClassNotFoundException with a cause set (i.e. exception.getCause returns
    // a null). This classloader is needed because janino will throw the exception directly if
    // the parent classloader throws a ClassNotFoundException with cause set instead of trying to
    // find other possible classes (see org.codehaus.janinoClassLoaderIClassLoader's
    // findIClass method). Please also see https://issues.apache.org/jira/browse/SPARK-15622 and
    // https://issues.apache.org/jira/browse/SPARK-11636.
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName(className)
    evaluator.setDefaultImports(importClassNames.toArray)
    evaluator.setExtendedClass(extendedClass)

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(debugSource, debugLines, debugVars)
      s"\n${CodeFormatter.format(code)}"
    })

    try {
      evaluator.cook("generated.java", code.body)
    } catch {
      case e: InternalCompilerException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        val maxLines = SQLConf.get.loggingMaxLinesForCodegen
        logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
        throw new InternalCompilerException(msg, e)
      case e: CompileException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        val maxLines = SQLConf.get.loggingMaxLinesForCodegen
        logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
        throw new CompileException(msg, e.getLocation)
    }

    val maxCodeSize = updateAndGetCompilationStats(evaluator)
    (evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass], maxCodeSize)
  }
}
