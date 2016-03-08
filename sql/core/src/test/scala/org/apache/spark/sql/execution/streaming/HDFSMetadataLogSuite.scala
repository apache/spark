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

package org.apache.spark.sql.execution.streaming

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.test.SharedSQLContext

class HDFSMetadataLogSuite extends SparkFunSuite with SharedSQLContext {

  test("basic") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      try {
        metadataLog.add(0, "batch0")
        assert(metadataLog.getLatest() === Some(0 -> "batch0"))
        assert(metadataLog.get(0) === Some("batch0"))
        assert(metadataLog.getLatest() === Some(0 -> "batch0"))
        assert(metadataLog.get(None, 0) === Array(0 -> "batch0"))

        metadataLog.add(1, "batch1")
        assert(metadataLog.get(0) === Some("batch0"))
        assert(metadataLog.get(1) === Some("batch1"))
        assert(metadataLog.getLatest() === Some(1 -> "batch1"))
        assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))

        // Adding the same batch does nothing
        metadataLog.add(1, "batch1-duplicated")
        assert(metadataLog.get(0) === Some("batch0"))
        assert(metadataLog.get(1) === Some("batch1"))
        assert(metadataLog.getLatest() === Some(1 -> "batch1"))
        assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))
      } finally {
        metadataLog.stop()
      }
    }
  }

  test("restart") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      metadataLog.add(0, "batch0")
      metadataLog.add(1, "batch1")
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))
      metadataLog.stop()

      val metadataLog2 = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.get(1) === Some("batch1"))
      assert(metadataLog2.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog2.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))
      metadataLog2.stop()
    }
  }

  test("corrupt metadata file") {
    withTempDir { temp =>
      val batchOneMetadataFile = new File(temp, "1").toPath

      // Firstly, get the correct content of batch 1 in binary
      val completedMetadataBytes = {
        val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
        try {
          metadataLog.add(0, "batch0")
          metadataLog.add(1, "batch1")
          Files.readAllBytes(batchOneMetadataFile)
        } finally {
          metadataLog.stop()
        }
      }

      // Delete the checksum file for batch 1. Otherwise HDFS will refuse to read the file as
      // the checksum is inconsistent with the file content
      new File(temp, ".1.crc").delete()

      for (len <- 0 until completedMetadataBytes.length) {
        // Then try to write partial content to the file.
        Files.write(batchOneMetadataFile, completedMetadataBytes.slice(0, len))
        val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
        try {
          // HDFSMetadataLog should ignore corrupt files
          assert(metadataLog.getLatest() === Some(0 -> "batch0"))
          assert(metadataLog.get(None, 1) === Array(0 -> "batch0"))
        } finally {
          metadataLog.stop()
        }
      }
    }
  }

  test("metadata directory collision") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      try {
        val e = intercept[SparkException] {
          new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
        }
        // Make sure we tell people the collision lock file
        assert(e.getMessage.contains(temp.getAbsolutePath))
      } finally {
        metadataLog.stop()
      }
    }
  }

  test("reopen the same metadata directory") {
    withTempDir { temp =>
      val metadataLog1 = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      metadataLog1.stop()
      val metadataLog2 = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      metadataLog2.stop()
    }
  }
}
