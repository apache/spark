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

package org.apache.spark.deploy

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path}
import java.util.jar.{JarEntry, JarOutputStream}
import javax.tools.{JavaFileObject, SimpleJavaFileObject, StandardLocation, ToolProvider}

import com.google.common.io.ByteStreams

import org.apache.spark.deploy.SparkSubmitUtils.MavenCoordinate

import scala.collection.JavaConversions._

private[deploy] object IvyTestUtils {

  /** 
   * Create the path for the jar and pom from the maven coordinate. Extension should be `jar` 
   * or `pom`.
   */
  private def pathFromCoordinate(
      artifact: MavenCoordinate,
      prefix: Path,
      ext: String,
      useIvyLayout: Boolean): Path = {
    val groupDirs = artifact.groupId.replace(".", File.separator)
    val artifactDirs = artifact.artifactId.replace(".", File.separator)
    val artifactPath = 
      if (!useIvyLayout) {
        Seq(groupDirs, artifactDirs, artifact.version).mkString(File.separator)
      } else {
        Seq(groupDirs, artifactDirs, artifact.version, ext + "s").mkString(File.separator)
      }
    new File(prefix.toFile, artifactPath).toPath
  }
  
  private def artifactName(artifact: MavenCoordinate, ext: String = ".jar"): String = {
    s"${artifact.artifactId}-${artifact.version}$ext"
  }

  /** Write the contents to a file to the supplied directory. */
  private def writeFile(dir: File, fileName: String, contents: String): File = {
    val outputFile = new File(dir, fileName)
    val outputStream = new FileOutputStream(outputFile)
    outputStream.write(contents.toCharArray.map(_.toByte))
    outputStream.close()
    outputFile
  }
  
  /** Create an example Python file. */
  private def createPythonFile(dir: File): File = {
    val contents =
      """def myfunc(x):
        |   return x + 1
      """.stripMargin
    writeFile(dir, "mylib.py", contents)
  }

  /** Compilable Java Source File */
  private class JavaSourceFromString(val file: File, val code: String)
    extends SimpleJavaFileObject(file.toURI, JavaFileObject.Kind.SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean) = code
  }

  /** Create a simple testable Class. */
  private def createJavaClass(dir: File, className: String, packageName: String): File = {
    val contents =
      s"""package $packageName;
        |
        |import java.lang.Integer;
        |
        |class $className implements java.io.Serializable {
        |
        | public $className() {}
        | 
        | public Integer myFunc(Integer x) {
        |   return x + 1;
        | }
        |}
      """.stripMargin
    val sourceFiles = Seq(new JavaSourceFromString(new File(dir, className + ".java"), contents))
    val compiler = ToolProvider.getSystemJavaCompiler
    val fileManager = compiler.getStandardFileManager(null, null, null)

    fileManager.setLocation(StandardLocation.CLASS_OUTPUT, List(dir))

    compiler.getTask(null, fileManager, null, null, null, sourceFiles).call()

    val fileName = s"${packageName.replace(".", File.separator)}${File.separator}$className.class"
    val result = new File(dir, fileName)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath)
    result
  }
  
  /** Helper method to write artifact information in the pom. */
  private def pomArtifactWriter(artifact: MavenCoordinate, tabCount: Int = 1): String = {
    var result = "\n" + "  " * tabCount + s"<groupId>${artifact.groupId}</groupId>"
    result += "\n" + "  " * tabCount + s"<artifactId>${artifact.artifactId}</artifactId>"
    result += "\n" + "  " * tabCount + s"<version>${artifact.version}</version>"
    result
  } 
  
  /** Create a pom file for this artifact. */
  private def createPom(
      dir: File, 
      artifact: MavenCoordinate, 
      dependencies: Option[Seq[MavenCoordinate]]): File = {
    var content = """
                |<?xml version="1.0" encoding="UTF-8"?>
                |<project xmlns="http://maven.apache.org/POM/4.0.0"
                |       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                |       xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                |       http://maven.apache.org/xsd/maven-4.0.0.xsd">
                |   <modelVersion>4.0.0</modelVersion>
                """.stripMargin.trim
    content += pomArtifactWriter(artifact)
    content += dependencies.map { deps => 
      val inside = deps.map { dep =>
        "\t<dependency>" + pomArtifactWriter(dep, 3) + "\n\t</dependency>"
      }.mkString("\n")
      "\n  <dependencies>\n" + inside + "\n  </dependencies>"
    }.getOrElse("")
    content += "\n</project>"
    writeFile(dir, artifactName(artifact, ".pom"), content.trim)
  }
  
  /** Create the jar for the given maven coordinate, using the supplied files. */
  private def packJar(
      dir: File, 
      artifact: MavenCoordinate, 
      files: Seq[(String, File)]): File = {
    val jarFile = new File(dir, artifactName(artifact))
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest())

    for (file <- files) {
      val jarEntry = new JarEntry(file._1)
      jarStream.putNextEntry(jarEntry)

      val in = new FileInputStream(file._2)
      ByteStreams.copy(in, jarStream)
      in.close()
    }
    jarStream.close()
    jarFileStream.close()

    jarFile
  }

  /**
   * Creates a jar and pom file, mocking a Maven repository. The root path can be supplied with
   * `tempDir`, dependencies can be created into the same repo, and python files can also be packed
   * inside the jar. 
   *  
   * @param artifact The maven coordinate to generate the jar and pom for.
   * @param dependencies List of dependencies this artifact might have to also create jars and poms.
   * @param tempDir The root folder of the repository
   * @param useIvyLayout whether to mock the Ivy layout for local repository testing
   * @param withPython Whether to pack python files inside the jar for extensive testing.
   * @return
   */
  private def createLocalRepository(
      artifact: MavenCoordinate,
      dependencies: Option[Seq[MavenCoordinate]] = None,
      tempDir: Option[Path] = None,
      useIvyLayout: Boolean = false,
      withPython: Boolean = false): Path = {
    // Where the root of the repository exists, and what Ivy will search in
    val tempPath = tempDir.getOrElse(Files.createTempDirectory(null))
    // Where to temporary class files and such
    val root = Files.createTempDirectory(tempPath, null).toFile
    val artifactPath = pathFromCoordinate(artifact, tempPath)
    Files.createDirectories(artifactPath)
    val className = "MyLib"

    val javaClass = createJavaClass(root, className, artifact.groupId)
    // A tuple of files representation in the jar, and the file
    val javaFile = (artifact.groupId.replace(".", "/") + "/" + javaClass.getName, javaClass)
    val allFiles = 
      if (withPython) {
        val pythonFile = createPythonFile(root)
        Seq(javaFile, (pythonFile.getName, pythonFile))
      } else {
        Seq(javaFile)
      }
    val jarFile = packJar(artifactPath.toFile, artifact, allFiles)
    assert(jarFile.exists(), "Problem creating Jar file")
    val pomFile = createPom(artifactPath.toFile, artifact, dependencies)
    assert(pomFile.exists(), "Problem creating Pom file")
    tempPath
  }

  private[databricks] def createLocalRepositoryForTests(
      artifact: String,
      dependencies: Option[String],
      withPython: Boolean = false): Path = {
    val art = parseCoordinates(artifact).head
    val deps = dependencies.map(parseCoordinates)
    val mainRepo = createLocalRepository(art, deps, withPython)
    deps.foreach { seq => seq.foreach { dep =>
      createLocalRepository(dep, None, false, Some(mainRepo))
    }}

    mainRepo
  }
}
