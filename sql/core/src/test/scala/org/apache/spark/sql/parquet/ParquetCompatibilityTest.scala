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

package org.apache.spark.sql.parquet
import java.io.File

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.QueryTest
import org.apache.spark.util.Utils

abstract class ParquetCompatibilityTest extends QueryTest with ParquetTest with BeforeAndAfterAll {
  protected var parquetStore: File = _

  /**
   * Optional path to a staging subdirectory which may be created during query processing
   * (Hive does this).
   * Parquet files under this directory will be ignored in [[readParquetSchema()]]
   * @return an optional staging directory to ignore when scanning for parquet files.
   */
  protected def stagingDir: Option[String] = None

  override protected def beforeAll(): Unit = {
    parquetStore = Utils.createTempDir(namePrefix = "parquet-compat_")
    parquetStore.delete()
  }

  override protected def afterAll(): Unit = {
    Utils.deleteRecursively(parquetStore)
  }

  def readParquetSchema(path: String): MessageType = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(configuration)
    val parquetFiles = fs.listStatus(fsPath).toSeq.filterNot { status =>
      status.getPath.getName.startsWith("_") ||
        stagingDir.map(status.getPath.getName.startsWith).getOrElse(false)
    }
    val footers = ParquetFileReader.readAllFootersInParallel(configuration, parquetFiles, true)
    footers.head.getParquetMetadata.getFileMetaData.getSchema
  }
}

object ParquetCompatibilityTest {
  def makeNullable[T <: AnyRef](i: Int)(f: => T): T = {
    if (i % 3 == 0) null.asInstanceOf[T] else f
  }
}
