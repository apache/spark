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

package org.apache.spark.sql.execution.datasources.binaryfile

import java.sql.Timestamp

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.{col, substring_index}
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types.LongType

class BinaryFileSuite extends QueryTest with SharedSQLContext with SQLTestUtils {
  import testImplicits._

  private lazy val filePath = testFile("test-data/text-partitioned")

  private lazy val fsFilePath = new Path(filePath)

  private lazy val fs = fsFilePath.getFileSystem(sparkContext.hadoopConfiguration)

  test("binary file test") {

    val resultDF = spark.read.format("binaryFile")
      .load(filePath)
      .select(
        substring_index(col("status.path"), "/", -1).as("path"),
        col("status.modification_time"),
        col("status.length"),
        col("content"),
        col("year")
      )

    val expectedRowSet = new collection.mutable.HashSet[Row]()

    for (partitionDirStatus <- fs.listStatus(fsFilePath)) {
      val dirPath = partitionDirStatus.getPath

      for (fileStatus <- fs.listStatus(dirPath)) {
        val fname = fileStatus.getPath.getName
        val flen = fileStatus.getLen
        val modificationTime = new Timestamp(fileStatus.getModificationTime)

        val fcontent = {
          val stream = fs.open(fileStatus.getPath)
          val content = try {
            ByteStreams.toByteArray(stream)
          } finally {
            Closeables.close(stream, true)
          }
          content
        }

        val partitionName = dirPath.getName.split("=")(1)
        val year = partitionName.toInt
        val row = Row(fname, modificationTime, flen, fcontent, year)
        expectedRowSet.add(row)
      }
    }

    val result = resultDF.collect()
    assert(Set(result: _*) === expectedRowSet.toSet)
  }

}
