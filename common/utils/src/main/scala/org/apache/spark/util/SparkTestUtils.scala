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

package org.apache.spark.util

import java.io.File
import java.net.{URI, URL}
import java.nio.file.Files
import java.util.Arrays
import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}

import scala.jdk.CollectionConverters._

private[spark] trait SparkTestUtils {
  // Adapted from the JavaCompiler.java doc examples
  private val SOURCE = JavaFileObject.Kind.SOURCE

  private def createURI(name: String) = {
    URI.create(s"string:///${name.replace(".", "/")}${SOURCE.extension}")
  }

  private[spark] class JavaSourceFromString(val name: String, val code: String)
      extends SimpleJavaFileObject(createURI(name), SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): String = code
  }

  /** Creates a compiled class with the source file. Class file will be placed in destDir. */
  def createCompiledClass(
      className: String,
      destDir: File,
      sourceFile: JavaSourceFromString,
      classpathUrls: Seq[URL]): File = {
    val compiler = ToolProvider.getSystemJavaCompiler

    // Calling this outputs a class file in pwd. It's easier to just rename the files than
    // build a custom FileManager that controls the output location.
    val options = if (classpathUrls.nonEmpty) {
      Seq(
        "-classpath",
        classpathUrls
          .map {
            _.getFile
          }
          .mkString(File.pathSeparator))
    } else {
      Seq.empty
    }
    compiler.getTask(null, null, null, options.asJava, null, Arrays.asList(sourceFile)).call()

    val fileName = className + ".class"
    val result = new File(fileName)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath())
    val out = new File(destDir, fileName)

    // renameTo cannot handle in and out files in different filesystems
    // use google's Files.move instead
    Files.move(result.toPath, out.toPath)

    assert(out.exists(), "Destination file not moved: " + out.getAbsolutePath())
    out
  }

  /** Creates a compiled class with the given name. Class file will be placed in destDir. */
  def createCompiledClass(
      className: String,
      destDir: File,
      toStringValue: String = "",
      baseClass: String = null,
      classpathUrls: Seq[URL] = Seq.empty,
      implementsClasses: Seq[String] = Seq.empty,
      extraCodeBody: String = "",
      packageName: Option[String] = None): File = {
    val extendsText = Option(baseClass).map { c => s" extends ${c}" }.getOrElse("")
    val implementsText =
      "implements " + (implementsClasses :+ "java.io.Serializable").mkString(", ")
    val packageText = packageName.map(p => s"package $p;\n").getOrElse("")
    val sourceFile = new JavaSourceFromString(
      className,
      s"""
         |$packageText
         |public class $className $extendsText $implementsText {
         |  @Override public String toString() { return "$toStringValue"; }
         |
         |  $extraCodeBody
         |}
        """.stripMargin)
    createCompiledClass(className, destDir, sourceFile, classpathUrls)
  }

}

private[spark] object SparkTestUtils extends SparkTestUtils
