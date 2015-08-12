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

import java.io.File

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.QueryTest
import org.apache.spark.util.Utils

/**
 * Helper class for testing Parquet compatibility.
 */
private[sql] abstract class ParquetCompatibilityTest extends QueryTest with ParquetTest {

  protected var parquetStore: File = _

  /**
   * Optional path to a staging subdirectory which may be created during query processing
   * (Hive does this).
   * Parquet files under this directory will be ignored in [[readParquetSchema()]]
   * @return an optional staging directory to ignore when scanning for parquet files.
   */
  protected def stagingDir: Option[String] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    parquetStore = Utils.createTempDir(namePrefix = "parquet-compat_")
    parquetStore.delete()
  }

  override protected def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(parquetStore)
    } finally {
      super.afterAll()
    }
  }

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
}

object ParquetCompatibilityTest {
  def makeNullable[T <: AnyRef](i: Int)(f: => T): T = {
    if (i % 3 == 0) null.asInstanceOf[T] else f
  }
}
