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

import java.io._

import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils


@SlowSQLTest
/**
 * Test suite to ingest some failures in RocksDB checkpoint */
class RocksDBCheckpointFailureInjectionSuite extends StreamTest
  with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS, classOf[RocksDBStateStoreProvider].getName)

  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    FailureInjectionFileSystem.failPreCopyFromLocalFileNameRegex = Seq.empty
    FailureInjectionFileSystem.failureCreateAtomicRegex = Seq.empty
    FailureInjectionFileSystem.shouldFailExist = false
  }

  implicit def toArray(str: String): Array[Byte] = if (str != null) str.getBytes else null

  test("Basic RocksDB SST File Upload Failure Handling") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf) { db =>
          db.put("version", "1.1")
          commitAndGetCheckpointId(db)

          FailureInjectionFileSystem.failPreCopyFromLocalFileNameRegex = Seq(".*sst")
          db.put("version", "2.1")
          intercept[IOException] {
            commitAndGetCheckpointId(db)
          }

          db.load(1)

          FailureInjectionFileSystem.failPreCopyFromLocalFileNameRegex = Seq.empty
          var ex = intercept[SparkException] {
            db.load(2)
          }
          checkError(
            ex,
            condition = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE",
            parameters = Map(
              "fileToRead" -> s"$remoteDir/2.changelog"
            )
          )

          db.load(0)
            FailureInjectionFileSystem.shouldFailExist = true
          var ex2 = intercept[IOException] {
            db.load(1)
          }
        }
      }
    }
  }

  test("Basic RocksDB Zip File Upload Failure Handling") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf) { db =>
          db.put("version", "1.1")
          commitAndGetCheckpointId(db)

          db.load(1)
          FailureInjectionFileSystem.failureCreateAtomicRegex = Seq(".*zip")
          db.put("version", "2.1")
          intercept[IOException] {
            commitAndGetCheckpointId(db)
          }

          db.load(1)

          FailureInjectionFileSystem.failureCreateAtomicRegex = Seq.empty
          var ex = intercept[SparkException] {
            db.load(2)
          }
          checkError(
            ex,
            condition = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE",
            parameters = Map(
              "fileToRead" -> s"$remoteDir/2.changelog"
            )
          )

          db.load(0)
          FailureInjectionFileSystem.shouldFailExist = true
          var ex2 = intercept[IOException] {
            db.load(1)
          }
        }
      }
    }
  }


  /**
   * This test is to simulate the case where a previous task had connectivity problem that couldn't
   * be killed or write zip file. Only after the later one is successfully committed, it come back
   * and write the zip file.
   * The final validation isn't necessary for V1 but we just would like to make sure
   * FailureInjectionCheckpointFileManager has correct behavior --  allows zip files to be delayed
   * to be written, so that the test for V1 is valid.
   */
  test("Zip File Overwritten by Previous Task Checkpoint V1") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf) { db =>
          db.put("version", "1.1")
          commitAndGetCheckpointId(db)

          db.load(1)
          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq(".*zip")
          db.put("version", "2.1")
          intercept[IOException] {
            commitAndGetCheckpointId(db)
          }

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1)

          db.put("version", "2.2")
          commitAndGetCheckpointId(db)

          assert(FailureInjectionFileSystem.delayedStreams.size == 1)
          FailureInjectionFileSystem.delayedStreams.foreach(_.close())
        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf) { db =>
          // Assuming previous 2.zip overwrites, we should see the previous value.
          assert(new String(db.get("version"), "UTF-8") == "2.1")
        }
      }
    }
   }

  /**
   * This test is to simulate the case where a previous task had connectivity problem that couldn't
   *  be killed or write zip file. Only after the later one is successfully committed, it comes back
   *  and write the zip file.
   *  */
  test("Zip File Overwritten by Previous Task Checkpoint V2") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        var checkpointId2: Option[String] = None
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true) { db =>
          db.put("version", "1.1")
          val checkpointId1 = commitAndGetCheckpointId(db)

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq(".*zip")
          db.load(1, checkpointId1)
          db.put("version", "2.1")
          intercept[IOException] {
            commitAndGetCheckpointId(db)
          }

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1, checkpointId1)

          db.put("version", "2.2")
          checkpointId2 = commitAndGetCheckpointId(db)

          assert(FailureInjectionFileSystem.delayedStreams.nonEmpty)
          FailureInjectionFileSystem.delayedStreams.foreach(_.close())
        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true,
          checkpointId = checkpointId2 ) { db =>
          assert(new String(db.get("version"), "UTF-8") == "2.2")
        }
      }
    }
  }

  /**
   * This test is to simulate the case where a previous task had connectivity problem that couldn't
   * be killed or write changelog file. Only after the later one is successfully committed, it comes
   * back and write the changelog file.
   * In the end, the test validates that the state store has the value of the old overwriting
   * changelog. This change is not necessary but we would like to be here to ensure that the failure
   * Injection code works properly so that the v2 test result is valid.
   */
  test("Changelog File Overwritten by Previous Task With Changelog Checkpoint V1") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "5") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = false) { db =>
          db.put("version", "1.1")
          commitAndGetCheckpointId(db)

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq(".*/2\\.changelog")

          db.load(1)
          db.put("version", "2.1")
          intercept[IOException] {
            commitAndGetCheckpointId(db)
          }

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1)

          db.put("version", "2.2")
          commitAndGetCheckpointId(db)

          assert(FailureInjectionFileSystem.delayedStreams.nonEmpty)
          FailureInjectionFileSystem.delayedStreams.foreach(_.close())

        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = false ) { db =>

          assert(new String(db.get("version"), "UTF-8") == "2.1")
        }
      }
    }
  }

  /**
   * This test is to simulate the case where a previous task had connectivity problem that couldn't
   * be killed or write changelog file. Only after the later one is successfully committed, it come
   * back and write the changelog file.
   *  */
  test("Changelog File Overwritten by Previous Task With Changelog Checkpoint V2") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "5") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))

        var checkpointId2: Option[String] = None
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true) { db =>
          db.put("version", "1.1")
          val checkpointId1 = commitAndGetCheckpointId(db)

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq(".*/2_.*changelog")

          db.load(1, checkpointId1)
          db.put("version", "2.1")
          intercept[IOException] {
            commitAndGetCheckpointId(db)
          }

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1, checkpointId1)

          db.put("version", "2.2")
          checkpointId2 = commitAndGetCheckpointId(db)

          assert(FailureInjectionFileSystem.delayedStreams.nonEmpty)
          FailureInjectionFileSystem.delayedStreams.foreach(_.close())

          db.load(1, checkpointId1)
          db.load(2, checkpointId2)
        }

        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true,
          checkpointId = checkpointId2) { db =>
          assert(new String(db.get("version"), "UTF-8") == "2.2")
        }
      }
    }
  }

  /**
   * This test is to simulate the case where
   * 1. There is a snapshot checkpoint scheduled
   * 2. The batch eventually failed
   * 3. Query is retried and moved forward
   * 4. The snapshot checkpoint succeeded
   * In checkpoint V2, this snapshot shouldn't take effective. Otherwise, it will break the strong
   * consistency guaranteed by V2.
   */
  test("Delay Snapshot V2") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        var checkpointId3: Option[String] = None
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true) { db =>
          db.put("version", "1.1")
          val checkpointId1 = commitAndGetCheckpointId(db)

          withDB(
            remoteDir.getAbsolutePath,
            version = 1,
            conf = conf,
            hadoopConf = hadoopConf,
            enableStateStoreCheckpointIds = true,
            checkpointId = checkpointId1) { db2 =>
            db2.put("version", "2.1")
            db2.commit()

            db.load(1, checkpointId1)
            db.put("version", "2.2")
            val checkpointId2 = commitAndGetCheckpointId(db)

            db.load(2, checkpointId2)
            db.put("foo", "bar")
            checkpointId3 = commitAndGetCheckpointId(db)

            db2.doMaintenance()
          }
        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 3,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true,
          checkpointId = checkpointId3) { db =>
          assert(new String(db.get("version"), "UTF-8") == "2.2")
          assert(new String(db.get("foo"), "UTF-8") == "bar")
        }
      }
    }
  }

  import testImplicits._

  /**
   * An integrated test where a previous changelog from a failed batch come back and finish
   * writing. In checkpoint V2, this changelog should be ignored.
   * Test it with both file renaming overwrite and not renaming overwrite.
   */
  Seq(false, true).foreach { ifAllowRenameOverwrite =>
    test(s"Job failure with changelog shows up ifAllowRenameOverwrite = $ifAllowRenameOverwrite") {
      val fmClass = "org.apache.spark.sql.execution.streaming.state." +
        "FailureInjectionCheckpointFileManager"
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
      val rocksdbChangelogCheckpointingConfKey =
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"
      FailureInjectionFileSystem.allowOverwriteInRename = ifAllowRenameOverwrite
      withTempDir { checkpointDir =>
        withSQLConf(
          rocksdbChangelogCheckpointingConfKey -> "true",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
          val inputData = MemoryStream[Int]
          val aggregated =
            inputData.toDF()
              .groupBy($"value")
              .agg(count("*"))
              .as[(Int, Long)]

          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq(".*/2_.*changelog")

          testStream(aggregated, Update)(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = Map(
                rocksdbChangelogCheckpointingConfKey -> "true",
                SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
                STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fmClass)),
            AddData(inputData, 3),
            CheckLastBatch((3, 1)),
            AddData(inputData, 3, 2),
            ExpectFailure[SparkException] { ex =>
              ex.getCause.getMessage.contains("CANNOT_WRITE_STATE_STORE.CANNOT_COMMIT")
            },
            AddData(inputData, 3, 1)
          )
          assert(FailureInjectionFileSystem.delayedStreams.nonEmpty)
          FailureInjectionFileSystem.delayedStreams.foreach(_.close())
          FailureInjectionFileSystem.delayedStreams = Seq.empty
          FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          testStream(aggregated, Update)(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = Map(
                rocksdbChangelogCheckpointingConfKey -> "true",
                SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
                STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fmClass)),
            AddData(inputData, 4),
            CheckLastBatch((3, 3), (1, 1), (4, 1)),
            StopStream
          )
        }
      }
    }
  }

  /**
   * An integreated test to cover this scenario:
   * 1. A batch is running and a snapshot checkpoint is scheduled
   * 2. The batch fails
   * 3. The query restarts
   * 4. The snapshot checkpoint succeeded
   * and validate that the snapshot checkpoint is not used in subsequent query restart.
   */
  test("Previous Maintenance Snapshot Checkpoint Overwrite") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureInjectionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    val rocksdbChangelogCheckpointingConfKey =
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"
    withTempDir { checkpointDir =>
      withSQLConf(
        rocksdbChangelogCheckpointingConfKey -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
        val inputData = MemoryStream[Int]
        val aggregated =
          inputData.toDF()
            .groupBy($"value")
            .agg(count("*"))
            .as[(Int, Long)]

        FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq(".*/*zip")

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = Map(
              rocksdbChangelogCheckpointingConfKey -> "true",
              SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
              SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "20",
              STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fmClass)),
          AddData(inputData, 3),
          CheckAnswer((3, 1)),
          AddData(inputData, 3, 2),
          AddData(inputData, 3, 1),
          CheckAnswer((3, 1), (3, 3), (2, 1), (1, 1)),
          AddData(inputData, 1),
          CheckAnswer((3, 1), (3, 3), (2, 1), (1, 1), (1, 2)),
          Execute { _ =>
            while (FailureInjectionFileSystem.delayedStreams.isEmpty) {
              Thread.sleep(1)
            }
            // If we call StoreStore.stop(), or let testStream() call it implicitly, it will
            // cause deadlock. This is a work-around here before we make StateStore.stop() not
            // deadlock.
            StateStore.stopMaintenanceTaskWithoutLock()
          },
          StopStream
        )
        FailureInjectionFileSystem.createAtomicDelayCloseRegex = Seq.empty

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = Map(
              rocksdbChangelogCheckpointingConfKey -> "true",
              SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
              STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fmClass)),
          AddData(inputData, 1),
          CheckAnswer((1, 3)),
          StopStream
        )
        assert(FailureInjectionFileSystem.delayedStreams.nonEmpty)
        FailureInjectionFileSystem.delayedStreams.foreach(_.close())
        FailureInjectionFileSystem.delayedStreams = Seq.empty

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = Map(
              rocksdbChangelogCheckpointingConfKey -> "true",
              SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
              STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fmClass)),
          AddData(inputData, 3, 1, 4),
          CheckAnswer((3, 4), (1, 4), (4, 1)),
          StopStream
        )
      }
    }
  }

  def commitAndGetCheckpointId(db: RocksDB): Option[String] = {
    val (v, ci) = db.commit()
    ci.stateStoreCkptId
  }

  def withDB[T](
      remoteDir: String,
      version: Int,
      conf: RocksDBConf,
      hadoopConf: Configuration = new Configuration(),
      enableStateStoreCheckpointIds: Boolean = false,
      checkpointId: Option[String] = None)(
      func: RocksDB => T): T = {
    var db: RocksDB = null
    try {
      db = FailureInjectionRocksDBStateStoreProvider.createRocksDBWithFaultInjection(
        remoteDir,
        conf = conf,
        localRootDir = Utils.createTempDir(),
        hadoopConf = hadoopConf,
        loggingId = s"[Thread-${Thread.currentThread.getId}]",
        useColumnFamilies = true,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        partitionId = 0)
      db.load(version, checkpointId)
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }
}
