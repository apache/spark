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
package org.apache.spark.sql.execution.datasources.v2

import java.io.FileNotFoundException

import org.apache.spark.{SparkException, SparkFunSuite, SparkUpgradeException}
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException

class FileDataSourceV2Suite extends SparkFunSuite {

  private val testFilePath = "/test/path/file.parquet"

  test("SPARK-56459: attachFilePath rethrows SparkUpgradeException directly") {
    val cause = new SparkUpgradeException(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME",
      Map("format" -> "Parquet", "config" -> "config", "option" -> "option"),
      null)
    val thrown = intercept[SparkUpgradeException] {
      FileDataSourceV2.attachFilePath(testFilePath, cause)
    }
    assert(thrown eq cause)
  }

  test("SPARK-56459: attachFilePath rethrows FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER") {
    val cause = new SparkException(
      errorClass = "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER",
      messageParameters = Map("path" -> testFilePath),
      cause = null)
    val thrown = intercept[SparkException] {
      FileDataSourceV2.attachFilePath(testFilePath, cause)
    }
    assert(thrown eq cause)
  }

  test("SPARK-56459: attachFilePath wraps SchemaColumnConvertNotSupportedException") {
    val cause = new SchemaColumnConvertNotSupportedException("col1", "INT32", "STRING")
    val thrown = intercept[SparkException] {
      FileDataSourceV2.attachFilePath(testFilePath, cause)
    }
    assert(thrown.getCondition == "FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH")
    assert(thrown.getCause eq cause)
  }

  test("SPARK-56459: attachFilePath wraps FileNotFoundException") {
    val cause = new FileNotFoundException("file not found")
    val thrown = intercept[SparkException] {
      FileDataSourceV2.attachFilePath(testFilePath, cause)
    }
    assert(thrown.getCondition == "FAILED_READ_FILE.FILE_NOT_EXIST")
    assert(thrown.getCause eq cause)
  }

  test("SPARK-56459: attachFilePath wraps NonFatal exceptions") {
    val cause = new RuntimeException("something went wrong")
    val thrown = intercept[SparkException] {
      FileDataSourceV2.attachFilePath(testFilePath, cause)
    }
    assert(thrown.getCondition == "FAILED_READ_FILE.NO_HINT")
    assert(thrown.getCause eq cause)
  }

  test("SPARK-56459: attachFilePath rethrows fatal errors directly") {
    val oom = new OutOfMemoryError("Java heap space")
    val thrown = intercept[OutOfMemoryError] {
      FileDataSourceV2.attachFilePath(testFilePath, oom)
    }
    assert(thrown eq oom)
  }

  test("SPARK-56459: attachFilePath rethrows StackOverflowError directly") {
    val soe = new StackOverflowError("stack overflow")
    val thrown = intercept[StackOverflowError] {
      FileDataSourceV2.attachFilePath(testFilePath, soe)
    }
    assert(thrown eq soe)
  }
}
