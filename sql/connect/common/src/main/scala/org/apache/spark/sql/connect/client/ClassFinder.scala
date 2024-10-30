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

import java.nio.file.{Files, LinkOption, Path, Paths}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Artifact

trait ClassFinder {
  def findClasses(): Iterator[Artifact]
}

/**
 * A generic [[ClassFinder]] implementation that traverses a specific REPL output directory.
 * @param _rootDir
 */
class REPLClassDirMonitor(_rootDir: String) extends ClassFinder {
  private val rootDir = Paths.get(_rootDir)
  require(rootDir.isAbsolute)
  require(Files.isDirectory(rootDir))

  override def findClasses(): Iterator[Artifact] = {
    Files
      .walk(rootDir)
      // Ignore symbolic links
      .filter(path => Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && isClass(path))
      .map[Artifact](path => toArtifact(path))
      .iterator()
      .asScala
  }

  private def toArtifact(path: Path): Artifact = {
    // Persist the relative path of the classfile
    Artifact.newClassArtifact(rootDir.relativize(path), new Artifact.LocalFile(path))
  }

  private def isClass(path: Path): Boolean = path.toString.endsWith(".class")
}
