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

package org.apache.spark

import scala.collection.JavaConversions._

import java.io.{FileInputStream, FileOutputStream, File}
import java.util.jar.{JarEntry, JarOutputStream}
import java.net.{URL, URI}
import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}

object TestUtils {

  /** Create a jar file that contains this set of files. All files will be located at the root
    * of the jar. */
  def createJar(files: Seq[File], jarFile: File): URL = {
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest())

    for (file <- files) {
      val jarEntry = new JarEntry(file.getName)
      jarStream.putNextEntry(jarEntry)

      val in = new FileInputStream(file)
      val buffer = new Array[Byte](10240)
      var nRead = 0
      while (nRead <= 0) {
        nRead = in.read(buffer, 0, buffer.length)
        jarStream.write(buffer, 0, nRead)
      }
      in.close()
    }
    jarStream.close()
    jarFileStream.close()

    jarFile.toURI.toURL
  }

  // Adapted from the JavaCompiler.java doc examples
  private val SOURCE = JavaFileObject.Kind.SOURCE
  private def createURI(name: String) = {
    URI.create(s"string:///${name.replace(".", "/")}${SOURCE.extension}")
  }
  private class JavaSourceFromString(val name: String, val code: String)
    extends SimpleJavaFileObject(createURI(name), SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean) = code
  }

  /** Creates a compiled class with the given name. Class file will be placed in destDir. */
  def createCompiledClass(className: String, destDir: File): File = {
    val compiler = ToolProvider.getSystemJavaCompiler
    val sourceFile = new JavaSourceFromString(className, s"public class $className {}")

    // Calling this outputs a class file in pwd. It's easier to just rename the file than
    // build a custom FileManager that controls the output location.
    compiler.getTask(null, null, null, null, null, Seq(sourceFile)).call()

    val fileName = className + ".class"
    val result = new File(fileName)
    if (!result.exists()) throw new Exception("Compiled file not found: " + fileName)
    val out = new File(destDir, fileName)
    result.renameTo(out)
    out
  }
}
