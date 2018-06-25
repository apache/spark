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

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{Arrays, Locale}
import javax.tools._
import javax.tools.JavaFileManager.Location
import javax.tools.JavaFileObject.Kind

import scala.collection.mutable

import org.codehaus.commons.compiler.{CompileException, Location => CompilerLocation}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, GeneratedClass}
import org.apache.spark.util.{ParentClassLoader, Utils}


class JavaCodeManager(fileManager: JavaFileManager)
    extends ForwardingJavaFileManager[JavaFileManager](fileManager) with Logging {

  // Holds a map between class names and `JavaCode`s (it has all the inner classes)
  val objects = mutable.Map[String, JavaCode]()

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
      throw new CompileException(s"Unknown source file found: $unknown", null)
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
      throw new CompileException(s"Failed to open a reader for $className", null)
    }
  }

  override def openOutputStream(): OutputStream = {
    outputStream
  }
}

class JDKDiagnosticListener extends DiagnosticListener[JavaFileObject] {
  override def report(diagnostic: Diagnostic[_ <: JavaFileObject]): Unit = {
    if (diagnostic.getKind == javax.tools.Diagnostic.Kind.ERROR) {
      val message = s"$diagnostic (${diagnostic.getCode()})"
      val loc = new CompilerLocation(
        diagnostic.getSource.toString,
        diagnostic.getLineNumber.toShort,
        diagnostic.getColumnNumber.toShort
      )

      // Wrap the exception in a RuntimeException, because "report()"
      // does not declare checked exceptions.
      throw new RuntimeException(new CompileException(message, loc))
    // } else if (logger.isTraceEnabled()) {
    //   logger.trace(diagnostic.toString() + " (" + diagnostic.getCode() + ")")
    }
  }
}

object JdkCompiler extends CompilerBase with Logging {
  val javaCompiler = {
    ToolProvider.getSystemJavaCompiler
  }

  private val compilerOptions = {
    val debugOption = new StringBuilder("-g:")
    if (this.debugSource) debugOption.append("source,")
    if (this.debugLines) debugOption.append("lines,")
    if (this.debugVars) debugOption.append("vars,")
    if ("-g".equals(debugOption)) {
      debugOption.append("none,")
    }
    Arrays.asList("-classpath", System.getProperty("java.class.path"), debugOption.init.toString())
  }

  private val listener = new JDKDiagnosticListener()

  private def javaCodeManager() = {
    val fm = javaCompiler.getStandardFileManager(listener, Locale.ROOT, StandardCharsets.UTF_8)
    new JavaCodeManager(fm)
  }

  override def compile(code: CodeAndComment): (GeneratedClass, Int) = {
    val clazzName = "GeneratedIterator"

    val importClasses = importClassNames.map(name => s"import $name;").mkString("\n")

    val codeWithImports =
      s"""
         |$importClasses
         |
         |public class $clazzName extends ${extendedClass.getName} {
         |${code.body}
         |}
       """.stripMargin

    val javaCode = JavaCode(clazzName, Some(codeWithImports))
    val fileManager = javaCodeManager()
    val task = javaCompiler.getTask(
      null, fileManager, listener, compilerOptions, null, Arrays.asList(javaCode))

    logDebug({
      s"\n${prefixLineNumbers(CodeFormatter.format(code))}"
    })

    try {
      if (!task.call()) {
        throw new CompileException("Compilation failed", null)
      }
    } catch {
      case e: RuntimeException =>
        // Unwrap the compilation exception wrapped at JDKDiagnosticListener and throw it.
        val cause = e.getCause
        if (cause != null) {
          cause.getCause match {
            case _: CompileException => throw cause.getCause.asInstanceOf[CompileException]
            case _: IOException => throw cause.getCause.asInstanceOf[IOException]
            case _ =>
          }
        }
        throw e
      case _: Throwable => throw new CompileException("Compilation failed", null)
    }

    // TODO: Needs to get the max bytecode size of generated methods
    val maxMethodBytecodeSize = updateAndGetCompilationStats(fileManager.objects.toMap)

    val clazz = fileManager.getClassLoader(null).loadClass(clazzName)
    (clazz.newInstance().asInstanceOf[GeneratedClass], maxMethodBytecodeSize)
  }

  private def updateAndGetCompilationStats(objects: Map[String, JavaCode]): Int = {
    val codeAttr = Utils.classForName("org.codehaus.janino.util.ClassFile$CodeAttribute")
    val codeAttrField = codeAttr.getDeclaredField("code")
    codeAttrField.setAccessible(true)
    /*
    val codeSizes = objects.foreach { case (_, javaCode) =>
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(javaCode.getBytecode.size)
      try {
        val cf = new ClassFile(new ByteArrayInputStream(javaCode.getBytecode))
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
    */
    0
  }
}
