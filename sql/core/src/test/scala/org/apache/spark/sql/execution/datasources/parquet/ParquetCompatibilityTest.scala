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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.QueryTest

/**
 * Helper class for testing Parquet compatibility.
 */
private[sql] abstract class ParquetCompatibilityTest extends QueryTest with ParquetTest {
  protected def readParquetSchema(path: String): MessageType = {
    readParquetSchema(path, { path => !path.getName.startsWith("_") })
  }

  protected def readParquetSchema(path: String, pathFilter: Path => Boolean): MessageType = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(configuration)
    val parquetFiles = fs.listStatus(fsPath, new PathFilter {
      override def accept(path: Path): Boolean = pathFilter(path)
    }).toSeq

    val footers = ParquetFileReader.readAllFootersInParallel(configuration, parquetFiles, true)
    footers.head.getParquetMetadata.getFileMetaData.getSchema
  }

  protected def logParquetSchema(path: String): Unit = {
    logInfo(
      s"""Schema of the Parquet file written by parquet-avro:
         |${readParquetSchema(path)}
       """.stripMargin)
  }
}

object ParquetCompatibilityTest {
  def makeNullable[T <: AnyRef](i: Int)(f: => T): T = {
    if (i % 3 == 0) null.asInstanceOf[T] else f
  }
}
