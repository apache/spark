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

import java.io.{ByteArrayOutputStream, OutputStream, Reader, StringReader}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{Arrays, Locale}
import javax.tools._
import javax.tools.JavaFileManager.Location
import javax.tools.JavaFileObject.Kind

import scala.collection.mutable

import com.sun.org.apache.xalan.internal.xsltc.compiler.CompilerException
import org.codehaus.commons.compiler.CompileException

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, GeneratedClass}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types._
import org.apache.spark.util.ParentClassLoader
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators


class JavaCodeManager(fileManager: JavaFileManager)
    extends ForwardingJavaFileManager[JavaFileManager](fileManager) with Logging {

  // Holds a map between class names and `JavaCode`s (it has all the inner classes)
  private val objects = mutable.Map[String, JavaCode]()

  private val classLoader = new ClassLoader(null) {

    // Loads a compile class into a current context class loader
    val parentLoader = new ParentClassLoader(Thread.currentThread().getContextClassLoader)

    private def loadAllObjects(): Unit = {
      objects.foreach { case (className, javaCode) =>
        try {
          parentLoader.loadClass(className)
        } catch {
          case _: ClassNotFoundException =>
            val bytecode = javaCode.getBytecode
            parentLoader.loadClass(className, bytecode, 0, bytecode.length)
        }
      }
    }

    override def findClass(name: String): Class[_] = {
      try {
        parentLoader.loadClass(name)
      } catch {
        case _: ClassNotFoundException =>
          loadAllObjects()
          parentLoader.loadClass(name)
      }
    }
  }

  override def getClassLoader(location: Location): ClassLoader = {
    classLoader
  }

  override def getJavaFileForOutput(
      location: Location,
      className: String,
      kind: Kind,
      sibling: FileObject): JavaFileObject = sibling match {
    case code: JavaCode =>
      logDebug(s"getJavaFileForOutput called: className=$className sibling=${code.className}")
      val javaCode = if (code.className != className) JavaCode(className) else code
      objects += className -> javaCode
      javaCode
    case unknown =>
      throw new CompilerException(s"Unknown source file found: $unknown")
  }
}

case class JavaCode(className: String, code: Option[String] = None)
    extends SimpleJavaFileObject(
      URI.create(s"string:///${className.replace('.', '/')}${Kind.SOURCE.extension}"),
      Kind.SOURCE) {

  // Holds compiled bytecode
  private val outputStream = new ByteArrayOutputStream()

  def getBytecode: Array[Byte] = outputStream.toByteArray

  override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = code.getOrElse("")

  override def openReader(ignoreEncodingErrors: Boolean): Reader = {
    code.map { c => new StringReader(c) }.getOrElse {
      throw new CompilerException(s"Failed to open a reader for $className")
    }
  }

  override def openOutputStream(): OutputStream = {
    outputStream
  }
}

object JdkCompiler extends CompilerBase {

  private val javaCompiler = {
    val compiler = ToolProvider.getSystemJavaCompiler
    if (compiler == null) {
      throw new RuntimeException(
        "JDK Java compiler not available - probably you're running Drill with a JRE and not a JDK")
    }
    compiler
  }

  private def javaCodeManager() = {
    val fm = javaCompiler.getStandardFileManager(null, Locale.ROOT, StandardCharsets.UTF_8)
    new JavaCodeManager(fm)
  }

  private val compilerOptions = Arrays.asList("-classpath", System.getProperty("java.class.path"))

  override def compile(code: CodeAndComment): (GeneratedClass, Int) = {
    val clazzName = "GeneratedIterator"
    val codeWithImports =
      s"""import ${classOf[Platform].getName};
         |import ${classOf[InternalRow].getName};
         |import ${classOf[UnsafeRow].getName};
         |import ${classOf[UnsafeProjection].getName};
         |import ${classOf[UTF8String].getName};
         |import ${classOf[Decimal].getName};
         |import ${classOf[CalendarInterval].getName};
         |import ${classOf[ArrayData].getName};
         |import ${classOf[PrefixComparators].getName};
         |import ${classOf[UnsafeArrayData].getName};
         |import ${classOf[MapData].getName};
         |import ${classOf[UnsafeMapData].getName};
         |import ${classOf[Expression].getName};
         |import ${classOf[TaskContext].getName};
         |import ${classOf[TaskKilledException].getName};
         |import ${classOf[InputMetrics].getName};
         |
         |public class $clazzName extends ${classOf[GeneratedClass].getName} {
         |${code.body}
         |}
       """.stripMargin

    val javaCode = JavaCode(clazzName, Some(codeWithImports))
    val fileManager = javaCodeManager()
    val task = javaCompiler.getTask(
      null, fileManager, null, compilerOptions, null, Arrays.asList(javaCode))
    if (!task.call()) {
      throw new CompileException("Compilation failed", null)
    }
    val clazz = fileManager.getClassLoader(null).loadClass(clazzName)
    (clazz.newInstance().asInstanceOf[GeneratedClass], 0)
  }
}
