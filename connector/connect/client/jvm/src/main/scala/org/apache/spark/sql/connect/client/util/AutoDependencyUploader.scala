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
package org.apache.spark.sql.connect.client.util

import java.nio.file.{Files, Path, Paths}
import java.util.Scanner

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.Artifact
import org.apache.spark.sql.connect.client.Artifact.LocalFile

object AutoDependencyUploader {

  lazy val connectClasspathEntries: Seq[Path] = {
    loadConnectClasspathEntries() :+
      filename(getClass.getProtectionDomain.getCodeSource.getLocation.getFile)
  }

  lazy val classpathEntries: Seq[Path] = {
    System.getProperty("java.class.path").split(':').toSeq.map(entry => Paths.get(entry))
  }

  lazy val userClasspathEntries: Seq[Path] = {
    val connectClasspathEntrySet = connectClasspathEntries.toSet
    classpathEntries.filterNot { entry =>
      connectClasspathEntrySet.contains(entry.getFileName)
    }
  }

  def userJars: Seq[Artifact] = {
    val seen = mutable.Set.empty[Path]
    userClasspathEntries.filter(p => isFileWithSuffix(p, ".jar"))
      .map(p => Artifact.newJarArtifact(p.getFileName, new LocalFile(p)))
      .filter(artifact => seen.add(artifact.path))
  }

  def userClasses: Seq[Artifact] = {
    userClasspathEntries
      .filter(p => Files.isDirectory(p))
      .flatMap { base =>
        val builder = Seq.newBuilder[Artifact]
        val stream = Files.walk(base)
        try {
          stream.filter(p => isFileWithSuffix(p, ".class")).forEach { path =>
            builder += Artifact.newClassArtifact(base.relativize(path), new LocalFile(path))
          }
        } finally {
          stream.close()
        }
        builder.result()
      }
  }

  def sync(spark: SparkSession): Unit = {
    spark.client.artifactManager.addArtifacts(userJars ++ userClasses)
  }

  private def filename(entry: String): Path = Paths.get(entry).getFileName

  private def isFileWithSuffix(path: Path, suffix: String): Boolean = {
    Files.isRegularFile(path) && path.toString.endsWith(suffix)
  }

  private def loadConnectClasspathEntries(): Seq[Path] = {
    val builder = Seq.newBuilder[Path]
    val stream = getClass.getClassLoader.getResourceAsStream("classpath")
    val scanner = new Scanner(stream, "UTF-8")
    scanner.useDelimiter(":")
    try {
      scanner.forEachRemaining(entry => builder += filename(entry))
    } finally {
      stream.close()
    }
    builder.result()
  }
}
