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

package org.apache.spark.sql.execution.datasources

import java.io.{ByteArrayOutputStream, File, FileOutputStream}
import java.util.zip.GZIPOutputStream

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.sql.{Dataset, Encoders, FakeFileSystemRequiringDSOption, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper

/**
 * The trait contains tests for all file-based data sources.
 * The tests that are not applicable to all file-based data sources should be placed to
 * [[org.apache.spark.sql.FileBasedDataSourceSuite]].
 */
trait CommonFileDataSourceSuite extends SQLHelper {
    self: AnyFunSuite => // scalastyle:ignore funsuite

  protected def spark: SparkSession
  protected def dataSourceFormat: String
  protected def inputDataset: Dataset[_] = spark.createDataset(Seq("abc"))(Encoders.STRING)

  test(s"Propagate Hadoop configs from $dataSourceFormat options to underlying file system") {
    withSQLConf(
      "fs.file.impl" -> classOf[FakeFileSystemRequiringDSOption].getName,
      "fs.file.impl.disable.cache" -> "true") {
      Seq(false, true).foreach { mergeSchema =>
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          val conf = Map("ds_option" -> "value", "mergeSchema" -> mergeSchema.toString)
          inputDataset
            .write
            .options(conf)
            .format(dataSourceFormat)
            .save(path)
          Seq(path, "file:" + path.stripPrefix("file:")).foreach { p =>
            val readback = spark
              .read
              .options(conf)
              .format(dataSourceFormat)
              .load(p)
            // Checks that read doesn't throw the exception from `FakeFileSystemRequiringDSOption`
            readback.write.mode("overwrite").format("noop").save()
          }
        }
      }
    }
  }

  protected def withCorruptFile(f: File => Unit): Unit = {
    val inputFile = File.createTempFile("input-", ".gz")
    try {
      // Create a corrupt gzip file
      val byteOutput = new ByteArrayOutputStream()
      val gzip = new GZIPOutputStream(byteOutput)
      try {
        gzip.write(Array[Byte](1, 2, 3, 4))
      } finally {
        gzip.close()
      }
      val bytes = byteOutput.toByteArray
      val o = new FileOutputStream(inputFile)
      try {
        // It's corrupt since we only write half of bytes into the file.
        o.write(bytes.take(bytes.length / 2))
      } finally {
        o.close()
      }
      f(inputFile)
    } finally {
      inputFile.delete()
    }
  }
}
