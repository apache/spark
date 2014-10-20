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

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.util.jar.{JarEntry, JarOutputStream}

import org.apache.commons.io.FilenameUtils

import scala.collection.JavaConversions._

import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}
import com.google.common.io.{ByteStreams, ByteSource, InputSupplier, Files}

import scala.collection.mutable

import org.apache.spark.util.Utils

/**
 * Utilities for tests. Included in main codebase since it's used by multiple
 * projects.
 *
 * TODO: See if we can move this to the test codebase by specifying
 * test dependencies between projects.
 */
private[spark] object TestUtils {

  /**
   * Create a jar that defines classes with the given names.
   *
   * Note: if this is used during class loader tests, class names should be unique
   * in order to avoid interference between tests.
   */
  def createJarWithClasses(classNames: Seq[String], value: String = ""): URL = {
    val tempDir = Utils.createTempDir()
    val files = for (name <- classNames) yield createCompiledClass(name, tempDir, value)
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    createJar(files, jarFile)
  }


  /**
   * Create a jar file that contains this set of files.
   *
   */
  def createJar(files: Seq[File],
                jarFile: File,
                flat:Boolean = true,
                stripPath: String = ""): URL = {
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest())
    val fprefix = if (!stripPath.isEmpty && !stripPath.endsWith("/")) stripPath + "/" else ""

    for (file <- files) {
      if (file.isDirectory && !flat) { // Skip directories in flat mode
        val name = file.getPath.stripPrefix(fprefix)
        val jarEntry = new JarEntry(name)
        jarEntry.setTime(file.lastModified())
        jarStream.putNextEntry(jarEntry)
        jarStream.closeEntry()
      } else if (file.isFile) { // Put file into jar file
        val name = if (flat) file.getName else file.getPath.stripPrefix(fprefix)
        val jarEntry = new JarEntry(name)
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
  def createCompiledClass(className: String, destDir: File, value: String = ""): File = {
    val compiler = ToolProvider.getSystemJavaCompiler
    val sourceFile = new JavaSourceFromString(className,
      "public class " + className + " implements java.io.Serializable {" +
      "  @Override public String toString() { return \"" + value + "\"; }}")

    // Calling this outputs a class file in pwd. It's easier to just rename the file than
    // build a custom FileManager that controls the output location.
    compiler.getTask(null, null, null, null, null, Seq(sourceFile)).call()

    val fileName = className + ".class"
    val result = new File(fileName)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath())
    val out = new File(destDir, fileName)

    // renameTo cannot handle in and out files in different filesystems
    // use google's Files.move instead
    Files.move(result, out)

    assert(out.exists(), "Destination file not moved: " + out.getAbsolutePath())
    out
  }

  /** Create a jar containing given classes.
    *
    * @param klazzes list of classes which will be in resulting jar
    * @param cl  classloader to access given classes
    */
  def createJarWithExistingClasses(klazzes: Seq[String], cl: ClassLoader): URL = {
    val tempDir = Files.createTempDir()
    tempDir.deleteOnExit()
    val files = for (klazz <- klazzes) yield createExistingClass(klazz, cl, tempDir)
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    createJar(files, jarFile, false, tempDir.getAbsolutePath)
  }

  /** Save an existing class loadable by given classloader into a file in destination directory
    *
    * @param klazz  name of class to save
    * @param cl  classloader for loading given class
    * @param destDir  directory where a class file will be saved
    * @return  return a reference to saved class file
    */
  def createExistingClass(klazz: String, cl: ClassLoader, destDir: File): File = {
    val klazzName = klazz.replace('.', '/') + ".class"

    val is = cl.getResourceAsStream(klazzName)
    val kDir  = new File(destDir, FilenameUtils.getPath(klazzName))
    val kFile = new File(kDir, FilenameUtils.getName(klazzName))
    kDir.mkdirs()
    // Copy content
    val os = Files.asByteSink(kFile).openStream()
    ByteStreams.copy(is, os)
    os.close()
    assert(kFile.exists(), "Destination file not created: " + kFile.getAbsolutePath())
    kFile
  }
}
