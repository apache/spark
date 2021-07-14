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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
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
 * Returns a list of file paths that are added to resources.
 * If file paths are provided, return the ones that are added to resources.
 */
case class ListFilesCommand(files: Seq[String] = Seq.empty[String]) extends LeafRunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("Results", StringType, nullable = false)() :: Nil
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fileList = sparkSession.sparkContext.listFiles()
    if (files.size > 0) {
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
    val jarList = sparkSession.sparkContext.listJars()
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
    val archiveList = sparkSession.sparkContext.listArchives()
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
