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
package org.apache.spark.sql.connect.client

import java.nio.file.Paths

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.util.SparkFileUtils

class ClassFinderSuite extends ConnectFunSuite {

  private val classResourcePath = commonResourcePath.resolve("artifact-tests")

  test("REPLClassDirMonitor functionality test") {
    val requiredClasses = Seq("Hello.class", "smallClassFile.class", "smallClassFileDup.class")
    requiredClasses.foreach(className =>
      assume(classResourcePath.resolve(className).toFile.exists))
    val copyDir = SparkFileUtils.createTempDir().toPath
    FileUtils.copyDirectory(classResourcePath.toFile, copyDir.toFile)
    val monitor = new REPLClassDirMonitor(copyDir.toAbsolutePath.toString)

    def checkClasses(monitor: REPLClassDirMonitor, additionalClasses: Seq[String] = Nil): Unit = {
      val expectedClassFiles = (requiredClasses ++ additionalClasses).map(name => Paths.get(name))

      val foundArtifacts = monitor.findClasses().toSeq
      assert(expectedClassFiles.forall { classPath =>
        foundArtifacts.exists(_.path == Paths.get("classes").resolve(classPath))
      })
    }

    checkClasses(monitor)

    // Add new class file into directory
    val subDir = SparkFileUtils.createTempDir(copyDir.toAbsolutePath.toString)
    val classToCopy = copyDir.resolve("Hello.class")
    val copyLocation = subDir.toPath.resolve("HelloDup.class")
    FileUtils.copyFile(classToCopy.toFile, copyLocation.toFile)

    checkClasses(monitor, Seq(s"${subDir.getName}/HelloDup.class"))
  }
}
