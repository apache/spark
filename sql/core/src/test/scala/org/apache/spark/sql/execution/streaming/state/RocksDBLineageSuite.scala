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

package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class RocksDBLineageSuite extends SharedSparkSession {
  private def newDB(root: String, enableCheckpointIds: Boolean): RocksDB = {
    val conf = RocksDBConf().copy(enableChangelogCheckpointing = true)
    new RocksDB(
      root,
      conf,
      localRootDir = Utils.createTempDir(),
      hadoopConf = new Configuration,
      useColumnFamilies = false,
      enableStateStoreCheckpointIds = enableCheckpointIds)
  }

  private def writeChangelogWithLineage(
      db: RocksDB,
      version: Long,
      uniqueId: String,
      lineage: Array[LineageItem]): Unit = {
    val writer = db.fileManager.getChangeLogWriter(
      version,
      useColumnFamilies = false,
      checkpointUniqueId = Some(uniqueId),
      stateStoreCheckpointIdLineage = Some(lineage))
    writer.commit()
  }

  test("getFullLineage: single changelog covers full range") {
    withTempDir { remoteDir =>
      val db = newDB(remoteDir.getAbsolutePath, enableCheckpointIds = true)
      try {
        val start = 3L
        val end = 5L
        val id3 = "i3"
        val id4 = "i4"
        val id5 = "i5"
        writeChangelogWithLineage(db, end, id5, Array(LineageItem(4, id4), LineageItem(3, id3)))

        val result = db.getFullLineage(start, end, Some(id5))
        assert(result.map(_.version).sameElements(Array(3L, 4L, 5L)))
        assert(result.map(_.checkpointUniqueId).sameElements(Array(id3, id4, id5)))
      } finally {
        db.close()
      }
    }
  }

  test("getFullLineage: multi-hop across changelog files") {
    withTempDir { remoteDir =>
      val db = newDB(remoteDir.getAbsolutePath, enableCheckpointIds = true)
      try {
        val start = 1L
        val end = 5L
        val id1 = "i1"; val id2 = "i2"; val id3 = "i3"; val id4 = "i4"; val id5 = "i5"
        writeChangelogWithLineage(db, 3, id3, Array(LineageItem(2, id2), LineageItem(1, id1)))
        writeChangelogWithLineage(db, 5, id5, Array(LineageItem(4, id4), LineageItem(3, id3)))

        val result = db.getFullLineage(start, end, Some(id5))
        assert(result.map(_.version).sameElements(Array(1L, 2L, 3L, 4L, 5L)))
        assert(result.map(_.checkpointUniqueId).sameElements(Array(id1, id2, id3, id4, id5)))
      } finally {
        db.close()
      }
    }
  }

  test("getFullLineage: multiple lineages exist for the same version") {
    withTempDir { remoteDir =>
      val db = newDB(remoteDir.getAbsolutePath, enableCheckpointIds = true)
      try {
        val start = 1L
        val end = 5L
        val id1 = "i1"; val id2 = "i2"; val id3 = "i3"; val id4 = "i4"; val id5 = "i5"
        writeChangelogWithLineage(db, 3, id3, Array(LineageItem(2, id2), LineageItem(1, id1)))
        writeChangelogWithLineage(db, 5, id5, Array(LineageItem(4, id4), LineageItem(3, id3)))
        // Insert a bad lineage for version 5
        // We should not use this lineage since we call getFullLineage with id5
        val badId4 = id4 + "bad"
        val badId5 = id5 + "bad"
        writeChangelogWithLineage(db, 5, badId5, Array(LineageItem(4, badId4)))

        val result = db.getFullLineage(start, end, Some(id5))
        assert(result.map(_.version).sameElements(Array(1L, 2L, 3L, 4L, 5L)))
        assert(result.map(_.checkpointUniqueId).sameElements(Array(id1, id2, id3, id4, id5)))
      } finally {
        db.close()
      }
    }
  }

  test("getFullLineage: start equals end returns single item") {
    withTempDir { remoteDir =>
      val db = newDB(remoteDir.getAbsolutePath, enableCheckpointIds = true)
      try {
        val result = db.getFullLineage(7, 7, Some("i7"))
        assert(result.map(_.version).sameElements(Array(7L)))
        assert(result.map(_.checkpointUniqueId).sameElements(Array("i7")))
      } finally {
        db.close()
      }
    }
  }

  test("getFullLineage: missing intermediate version triggers validation error") {
    withTempDir { remoteDir =>
      val db = newDB(remoteDir.getAbsolutePath, enableCheckpointIds = true)
      try {
        writeChangelogWithLineage(db, 5, "i5", Array(LineageItem(3, "i3")))
        val ex = intercept[SparkException] {
          db.getFullLineage(3, 5, Some("i5"))
        }
        checkError(
          ex,
          condition = "CANNOT_LOAD_STATE_STORE.INVALID_CHECKPOINT_LINEAGE",
          parameters = Map(
            "lineage" -> "3:i3 5:i5",
            "message" -> "Lineage versions are not increasing by one."
          )
        )
      } finally {
        db.close()
      }
    }
  }

  test("getFullLineage: no progress in lineage triggers guard error") {
    withTempDir { remoteDir =>
      val db = newDB(remoteDir.getAbsolutePath, enableCheckpointIds = true)
      try {
        writeChangelogWithLineage(db, 5, "i5", Array.empty)
        val ex = intercept[SparkException] {
          db.getFullLineage(3, 5, Some("i5"))
        }
        checkError(
          ex,
          condition = "CANNOT_LOAD_STATE_STORE.INVALID_CHECKPOINT_LINEAGE",
          parameters = Map(
            "lineage" -> "5:i5",
            "message" -> "Cannot find version smaller than 5 in lineage."
          )
        )
      } finally {
        db.close()
      }
    }
  }
}
