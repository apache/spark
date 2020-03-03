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
import java.net.URI

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Adds a jar to the current session so it can be used (for UDFs or serdes).
 */
case class AddJarCommand(path: String) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", IntegerType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.resourceLoader.addJar(path)
    Seq.empty[Row]
  }
}

/**
 * Adds a file to the current session so it can be used.
 */
case class AddFileCommand(path: String) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val recursive = !sparkSession.sessionState.conf.addSingleFileInAddFile
    sparkSession.sparkContext.addFile(path, recursive)
    Seq.empty[Row]
  }
}

/**
 * Returns a list of file paths that are added to resources.
 * If file paths are provided, return the ones that are added to resources.
 */
case class ListFilesCommand(files: Seq[String] = Seq.empty[String]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("Results", StringType, nullable = false)() :: Nil
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fileList = sparkSession.sparkContext.listFiles()
    if (files.size > 0) {
      files.map { f =>
        val uri = new URI(f)
        val schemeCorrectedPath = uri.getScheme match {
          case null | "local" => new File(f).getCanonicalFile.toURI.toString
          case _ => f
        }
        new Path(schemeCorrectedPath).toUri.toString
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
case class ListJarsCommand(jars: Seq[String] = Seq.empty[String]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("Results", StringType, nullable = false)() :: Nil
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val jarList = sparkSession.sparkContext.listJars()
    if (jars.nonEmpty) {
      for {
        jarName <- jars.map(f => new Path(f).getName)
        jarPath <- jarList if jarPath.contains(jarName)
      } yield Row(jarPath)
    } else {
      jarList.map(Row(_))
    }
  }
}
