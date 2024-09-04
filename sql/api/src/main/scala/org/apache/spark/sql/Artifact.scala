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

package org.apache.spark.sql

import java.io.{ByteArrayInputStream, InputStream, PrintStream}
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.Artifact.LocalData
import org.apache.spark.sql.util.ArtifactUtils
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.MavenUtils


private[sql] class Artifact private(val path: Path, val storage: LocalData) {
  require(!path.isAbsolute, s"Bad path: $path")

  lazy val size: Long = storage match {
    case localData: LocalData => localData.size
  }
}

private[sql] object Artifact {
  val CLASS_PREFIX: Path = Paths.get("classes")
  val JAR_PREFIX: Path = Paths.get("jars")
  val CACHE_PREFIX: Path = Paths.get("cache")

  def newArtifactFromExtension(
      fileName: String,
      targetFilePath: Path,
      storage: LocalData): Artifact = {
    fileName match {
      case jar if jar.endsWith(".jar") =>
        newJarArtifact(targetFilePath, storage)
      case cf if cf.endsWith(".class") =>
        newClassArtifact(targetFilePath, storage)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported file format: $other")
    }
  }

  def parseArtifacts(uri: URI): Seq[Artifact] = {
    // Currently only local files with extensions .jar and .class are supported.
    uri.getScheme match {
      case "file" =>
        val path = Paths.get(uri)
        val artifact = Artifact.newArtifactFromExtension(
          path.getFileName.toString,
          path.getFileName,
          new LocalFile(path))
        Seq[Artifact](artifact)

      case "ivy" =>
        newIvyArtifacts(uri)

      case other =>
        throw new UnsupportedOperationException(s"Unsupported scheme: $other")
    }
  }

  def newJarArtifact(targetFilePath: Path, storage: LocalData): Artifact = {
    newArtifact(JAR_PREFIX, ".jar", targetFilePath, storage)
  }

  def newClassArtifact(targetFilePath: Path, storage: LocalData): Artifact = {
    newArtifact(CLASS_PREFIX, ".class", targetFilePath, storage)
  }

  def newCacheArtifact(id: String, storage: LocalData): Artifact = {
    newArtifact(CACHE_PREFIX, "", Paths.get(id), storage)
  }

  def newIvyArtifacts(uri: URI): Seq[Artifact] = {
    implicit val printStream: PrintStream = System.err

    val authority = uri.getAuthority
    if (authority == null) {
      throw new IllegalArgumentException(
        s"Invalid Ivy URI authority in uri ${uri.toString}:" +
          " Expected 'org:module:version', found null.")
    }
    if (authority.split(":").length != 3) {
      throw new IllegalArgumentException(
        s"Invalid Ivy URI authority in uri ${uri.toString}:" +
          s" Expected 'org:module:version', found $authority.")
    }

    val (transitive, exclusions, repos) = MavenUtils.parseQueryParams(uri)

    val exclusionsList: Seq[String] =
      if (!StringUtils.isBlank(exclusions)) {
        exclusions.split(",").toImmutableArraySeq
      } else {
        Nil
      }

    val ivySettings = MavenUtils.buildIvySettings(Some(repos), None)

    val jars = MavenUtils.resolveMavenCoordinates(
      authority,
      ivySettings,
      transitive = transitive,
      exclusions = exclusionsList)
    jars.map(p => Paths.get(p)).map(path => newJarArtifact(path.getFileName, new LocalFile(path)))
  }

  private def newArtifact(
      prefix: Path,
      requiredSuffix: String,
      targetFilePath: Path,
      storage: LocalData): Artifact = {
    require(targetFilePath.toString.endsWith(requiredSuffix))
    new Artifact(ArtifactUtils.concatenatePaths(prefix, targetFilePath), storage)
  }

  /**
   * Payload stored on this machine.
   */
  sealed trait LocalData {
    def stream: InputStream

    def size: Long
  }

  /**
   * Payload stored in a local file.
   */
  class LocalFile(val path: Path) extends LocalData {
    override def size: Long = Files.size(path)

    override def stream: InputStream = Files.newInputStream(path)
  }

  /**
   * Payload stored in memory.
   */
  class InMemory(bytes: Array[Byte]) extends LocalData {
    override def size: Long = bytes.length

    override def stream: InputStream = new ByteArrayInputStream(bytes)
  }

}
