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

package org.apache.spark.sql.execution.command

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

/**
 * Adds a jar to the current session so it can be used (for UDFs or serdes).
 */
case class AddJarsCommand(paths: Seq[String]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    paths.foreach(sparkSession.sessionState.resourceLoader.addJar(_))
    Seq.empty[Row]
  }
}

/**
 * Adds a file to the current session so it can be used.
 */
case class AddFilesCommand(paths: Seq[String]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val recursive = !sparkSession.sessionState.conf.addSingleFileInAddFile
    paths.foreach(sparkSession.sparkContext.addFile(_, recursive))
    Seq.empty[Row]
  }
}

/**
 * Adds an archive to the current session so it can be used.
 */
case class AddArchivesCommand(paths: Seq[String]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    paths.foreach(sparkSession.sparkContext.addArchive(_))
    Seq.empty[Row]
  }
}

/**
 * The jars, files and archives added to resources, as a session lists them. When the artifacts of
 * the session are isolated, as with Spark Connect and with Scala and Java sessions without Hive
 * support, these are those added to the session and those added to the
 * [[org.apache.spark.SparkContext]] outside of any session, such as with `--jars`, `--files` and
 * `--archives`, but not those of the other sessions. Otherwise, these are all those added to the
 * [[org.apache.spark.SparkContext]].
 */
private object SessionResources {
  // The key of what is added to the SparkContext outside of any session.
  private val DefaultKey = "default"

  private def list(
      sparkSession: SparkSession,
      added: SparkContext => collection.Map[String, collection.Map[String, Long]],
      all: SparkContext => Seq[String]): Seq[String] = {
    val sc = sparkSession.sparkContext
    Option(sparkSession.artifactManager.state) match {
      case Some(state) =>
        Seq(state.uuid, DefaultKey).flatMap(added(sc).get).flatMap(_.keys).distinct
      case None => all(sc)
    }
  }

  def files(sparkSession: SparkSession): Seq[String] =
    list(sparkSession, _.addedFiles, _.listFiles())

  def jars(sparkSession: SparkSession): Seq[String] =
    list(sparkSession, _.addedJars, _.listJars())

  def archives(sparkSession: SparkSession): Seq[String] =
    list(sparkSession, _.addedArchives, _.listArchives())
}

/**
 * Returns a list of file paths that are added to resources.
 * If file paths are provided, return the ones that are added to resources.
 */
case class ListFilesCommand(files: Seq[String] = Seq.empty[String]) extends LeafRunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("Results", StringType, nullable = false)() :: Nil
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fileList = SessionResources.files(sparkSession)
    if (files.nonEmpty) {
      files.map { f =>
        val uri = Utils.resolveURI(f)
        uri.getScheme match {
          case null | "local" | "file" => new File(uri).getCanonicalFile.toURI.toString
          case _ => f
        }
      }.collect {
        case f if fileList.contains(f) => f
      }.map(Row(_))
    } else {
      fileList.map(Row(_))
    }
  }
}

/**
 * Returns a list of jar files that are added to resources.
 * If jar files are provided, return the ones that are added to resources.
 */
case class ListJarsCommand(jars: Seq[String] = Seq.empty[String]) extends LeafRunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("Results", StringType, nullable = false)() :: Nil
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val jarList = SessionResources.jars(sparkSession)
    if (jars.nonEmpty) {
      for {
        jarName <- jars.map(f => Utils.resolveURI(f).toString.split("/").last)
        jarPath <- jarList if jarPath.contains(jarName)
      } yield Row(jarPath)
    } else {
      jarList.map(Row(_))
    }
  }
}

/**
 * Returns a list of archive paths that are added to resources.
 * If archive paths are provided, return the ones that are added to resources.
 */
case class ListArchivesCommand(archives: Seq[String] = Seq.empty[String])
  extends LeafRunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("Results", StringType, nullable = false)() :: Nil
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val archiveList = SessionResources.archives(sparkSession)
    if (archives.nonEmpty) {
      for {
        archiveName <- archives.map(f => Utils.resolveURI(f).toString.split("/").last)
        archivePath <- archiveList if archivePath.contains(archiveName)
      } yield Row(archivePath)
    } else {
      archiveList.map(Row(_))
    }
  }
}
