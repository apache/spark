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

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.nio.file.Files
import java.util.Arrays
import java.util.jar.{JarEntry, JarOutputStream}
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

  /**
   * Compile Java source code and package the resulting class files into a JAR.
   * Supports classes with package declarations - the JAR will contain the proper
   * directory structure (e.g., org/apache/spark/Foo.class).
   *
   * @param sources map of fully-qualified class name to Java source code
   * @param jarFile the JAR file to create
   * @param classpathUrls additional classpath URLs needed for compilation
   * @return URL of the created JAR file
   */
  def createJarWithJavaSources(
      sources: Map[String, String],
      jarFile: File,
      classpathUrls: Seq[URL] = Seq.empty): URL = {
    val compiler = ToolProvider.getSystemJavaCompiler
    val classDir = Files.createTempDirectory("spark-test-classes").toFile

    val sourceFiles = sources.map { case (name, code) =>
      new JavaSourceFromString(name, code)
    }.toList

    val options = Seq("-d", classDir.getAbsolutePath) ++ (
      if (classpathUrls.nonEmpty) {
        Seq("-classpath", classpathUrls.map(_.getFile).mkString(File.pathSeparator))
      } else Seq.empty
    )

    val success = compiler.getTask(
      null, null, null, options.asJava, null,
      java.util.Arrays.asList(sourceFiles: _*)).call()
    assert(success, s"Compilation failed for: ${sources.keys.mkString(", ")}")

    // Collect all .class files under classDir
    val classFiles = listFilesRecursively(classDir).filter(_.getName.endsWith(".class"))

    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    try {
      for (classFile <- classFiles) {
        val entryName = classDir.toPath.relativize(classFile.toPath).toString.replace('\\', '/')
        jarStream.putNextEntry(new JarEntry(entryName))
        val in = new FileInputStream(classFile)
        try { in.transferTo(jarStream) } finally { in.close() }
      }
    } finally {
      jarStream.close()
    }

    SparkFileUtils.deleteRecursively(classDir)
    jarFile.toURI.toURL
  }

  /**
   * Compile Scala source files and package the resulting class files
   * into a JAR.
   *
   * @param sourceFiles Scala source files to compile
   * @param jarFile the JAR file to create
   * @param classpathUrls additional classpath URLs needed for compilation
   * @param excludeClassPrefixes class name prefixes to exclude from the
   *   JAR (e.g., Seq("A") excludes A.class, A$.class)
   */
  def createJarWithScalaSources(
      sourceFiles: Seq[File],
      jarFile: File,
      classpathUrls: Seq[URL] = Seq.empty,
      excludeClassPrefixes: Seq[String] = Seq.empty): URL = {
    val classDir = Files.createTempDirectory("spark-test-scala-classes").toFile
    val compilerClass = SparkClassUtils.classForName[AnyRef]("scala.tools.nsc.Main")
    val processMethod = compilerClass.getMethod("process", classOf[Array[String]])

    val cpStr = classpathUrls.map(_.getFile).mkString(File.pathSeparator)
    val args = Array("-classpath", cpStr, "-d", classDir.getAbsolutePath) ++
      sourceFiles.map(_.getAbsolutePath)

    val success = processMethod.invoke(null, args).asInstanceOf[Boolean]
    assert(success, s"Scala compilation failed for: ${sourceFiles.map(_.getName).mkString(", ")}")

    try {
      val classFiles = listFilesRecursively(classDir).filter { f =>
        f.getName.endsWith(".class") && !excludeClassPrefixes.exists(p =>
          f.getName == s"$p.class" || f.getName.startsWith(s"$p$$"))
      }

      jarFile.getParentFile.mkdirs()
      val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
      try {
        for (classFile <- classFiles) {
          val entryName = classDir.toPath.relativize(classFile.toPath).toString.replace('\\', '/')
          jarStream.putNextEntry(new JarEntry(entryName))
          val in = new FileInputStream(classFile)
          try { in.transferTo(jarStream) } finally { in.close() }
        }
      } finally {
        jarStream.close()
      }
    } finally {
      SparkFileUtils.deleteRecursively(classDir)
    }

    jarFile.toURI.toURL
  }

  private def listFilesRecursively(dir: File): Seq[File] = {
    val children = dir.listFiles()
    if (children == null) {
      Seq.empty
    } else {
      children.flatMap { f =>
        if (f.isDirectory) { listFilesRecursively(f) } else { Seq(f) }
      }.toSeq
    }
  }

}

private[spark] object SparkTestUtils extends SparkTestUtils
