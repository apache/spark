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
/** Test suite to inject some failures in RocksDB checkpoint */
class RocksDBCheckpointFailureInjectionSuite extends StreamTest
  with SharedSparkSession {

  private val fileManagerClassName = classOf[FailureInjectionCheckpointFileManager].getName

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS, classOf[RocksDBStateStoreProvider].getName)

  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  /**
   * create a temp dir and register it to failure injection file system and remove it in the end
   * The function can be moved to StreamTest if it is used by other tests too.
   * @param f the function to run with the temp dir and the injection state
   */
  def withTempDirAllowFailureInjection(f: (File, FailureInjectionState) => Unit): Unit = {
    withTempDir { dir =>
      val injectionState = FailureInjectionFileSystem.registerTempPath(dir.getPath)
      try {
        f(dir, injectionState)
      } finally {
        FailureInjectionFileSystem.removePathFromTempToInjectionState(dir.getPath)
      }
    }
  }

  implicit def toArray(str: String): Array[Byte] = if (str != null) str.getBytes else null

  case class FailureConf(ifEnableStateStoreCheckpointIds: Boolean, fileType: String) {
    override def toString: String = {
      s"ifEnableStateStoreCheckpointIds = $ifEnableStateStoreCheckpointIds, " +
        s"fileType = $fileType"
    }
  }

  Seq(
    FailureConf(ifEnableStateStoreCheckpointIds = false, "zip"),
    FailureConf(ifEnableStateStoreCheckpointIds = false, "sst"),
    FailureConf(ifEnableStateStoreCheckpointIds = true, "zip"),
    FailureConf(ifEnableStateStoreCheckpointIds = true, "sst")).foreach { testConf =>
    test(s"Basic RocksDB SST File Upload Failure Handling $testConf") {
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
      withTempDirAllowFailureInjection { (remoteDir, injectionState) =>
        withSQLConf(
          RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
          val conf = RocksDBConf(StateStoreConf(SQLConf.get))
          withDB(
            remoteDir.getAbsolutePath,
            version = 0,
            conf = conf,
            hadoopConf = hadoopConf,
            enableStateStoreCheckpointIds = testConf.ifEnableStateStoreCheckpointIds) { db =>
            db.put("version", "1.1")
            val checkpointId1 = commitAndGetCheckpointId(db)

            if (testConf.fileType == "sst") {
              injectionState.failPreCopyFromLocalFileNameRegex = Seq(".*sst")
            } else {
              assert(testConf.fileType == "zip")
              injectionState.failureCreateAtomicRegex = Seq(".*zip")
            }
            db.put("version", "2.1")
            var checkpointId2: Option[String] = None
            intercept[IOException] {
              checkpointId2 = commitAndGetCheckpointId(db)
            }

            db.load(1, checkpointId1)

            injectionState.failPreCopyFromLocalFileNameRegex = Seq.empty
            injectionState.failureCreateAtomicRegex = Seq.empty
            // When ifEnableStateStoreCheckpointIds is true, checkpointId is not available
            // to load version 2. If we use None, it will throw a Runtime error. We probably
            // should categorize this error.
            // TODO test ifEnableStateStoreCheckpointIds = true case after it is fixed.
            if (!testConf.ifEnableStateStoreCheckpointIds) {
              // Make sure that the checkpoint can't be loaded as some files aren't uploaded
              // correctly.
              val ex = intercept[SparkException] {
                db.load(2, checkpointId2)
              }
              checkError(
                ex,
                condition = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE",
                parameters = Map(
                  "fileToRead" -> s"$remoteDir/2.changelog"
                )
              )
            }

            db.load(0)
            injectionState.shouldFailExist = true
            intercept[IOException] {
              db.load(1, checkpointId1)
            }
          }
        }
      }
    }
  }

  /**
   * This test is to simulate the case where a previous task had connectivity problem that couldn't
   * be killed or write zip file. Only after the later one is successfully committed, it comes back
   * and write the zip file.
   */
  Seq(true, false).foreach { ifEnableStateStoreCheckpointIds =>
    test(
      "Zip File Overwritten by Previous Task Checkpoint " +
      s"ifEnableStateStoreCheckpointIds = $ifEnableStateStoreCheckpointIds") {
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
      withTempDirAllowFailureInjection { (remoteDir, injectionState) =>
        withSQLConf(
          RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
          val conf = RocksDBConf(StateStoreConf(SQLConf.get))

          var checkpointId2: Option[String] = None
          withDB(
            remoteDir.getAbsolutePath,
            version = 0,
            conf = conf,
            hadoopConf = hadoopConf,
            enableStateStoreCheckpointIds = ifEnableStateStoreCheckpointIds) { db =>
            db.put("version", "1.1")
            val checkpointId1 = commitAndGetCheckpointId(db)

            db.load(1, checkpointId1)
            injectionState.createAtomicDelayCloseRegex = Seq(".*zip")
            db.put("version", "2.1")

            intercept[IOException] {
              commitAndGetCheckpointId(db)
            }

            injectionState.createAtomicDelayCloseRegex = Seq.empty

            db.load(1, checkpointId1)

            db.put("version", "2.2")
            checkpointId2 = commitAndGetCheckpointId(db)

            assert(injectionState.delayedStreams.nonEmpty)
            injectionState.delayedStreams.foreach(_.close())
          }
          withDB(
            remoteDir.getAbsolutePath,
            version = 2,
            conf = conf,
            hadoopConf = hadoopConf,
            enableStateStoreCheckpointIds = ifEnableStateStoreCheckpointIds,
            checkpointId = checkpointId2) { db =>
            if (ifEnableStateStoreCheckpointIds) {
              // If checkpointV2 is used, writing a checkpoint file from a previously failed
              // batch should be ignored.
              assert(new String(db.get("version"), "UTF-8") == "2.2")
            } else {
              // Assuming previous 2.zip overwrites, we should see the previous value.
              // This validation isn't necessary here but we just would like to make sure
              // FailureInjectionCheckpointFileManager has correct behavior --  allows zip files
              // to be delayed to be written, so that the test for
              // ifEnableStateStoreCheckpointIds = true is valid.
              assert(new String(db.get("version"), "UTF-8") == "2.1")
            }
          }
        }
      }
    }
  }

  /**
   * This test is to simulate the case where a previous task had connectivity problem that couldn't
   * be killed or write changelog file. Only after the later one is successfully committed, it comes
   * back and write the changelog file.
   *  */
  Seq(false, true).foreach { ifEnableStateStoreCheckpointIds =>
    test(
      "Changelog File Overwritten by Previous Task With Changelog Checkpoint " +
      s"ifEnableStateStoreCheckpointIds = $ifEnableStateStoreCheckpointIds") {
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
      withTempDirAllowFailureInjection { (remoteDir, injectionState) =>
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
            enableStateStoreCheckpointIds = ifEnableStateStoreCheckpointIds) { db =>
            db.put("version", "1.1")
            val checkpointId1 = commitAndGetCheckpointId(db)

            injectionState.createAtomicDelayCloseRegex = Seq(".*/2.*changelog")

            db.load(1, checkpointId1)
            db.put("version", "2.1")
            intercept[IOException] {
              commitAndGetCheckpointId(db)
            }

            injectionState.createAtomicDelayCloseRegex = Seq.empty

            db.load(1, checkpointId1)

            db.put("version", "2.2")
            checkpointId2 = commitAndGetCheckpointId(db)

            assert(injectionState.delayedStreams.nonEmpty)
            injectionState.delayedStreams.foreach(_.close())

            db.load(1, checkpointId1)
            db.load(2, checkpointId2)
          }

          withDB(
            remoteDir.getAbsolutePath,
            version = 2,
            conf = conf,
            hadoopConf = hadoopConf,
            enableStateStoreCheckpointIds = ifEnableStateStoreCheckpointIds,
            checkpointId = checkpointId2) { db =>
            if (ifEnableStateStoreCheckpointIds) {
              // If checkpointV2 is used, writing a checkpoint file from a previously failed
              // batch should be ignored.
              assert(new String(db.get("version"), "UTF-8") == "2.2")
            } else {
              // This check is not necessary. But we would like to validate the behavior of
              // FailureInjectionFileSystem to ensure the ifEnableStateStoreCheckpointIds = true
              // case is valid.
              assert(new String(db.get("version"), "UTF-8") == "2.1")
            }
          }
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
   * In checkpoint V2, this snapshot shouldn't take effect. Otherwise, it will break the strong
   * consistency guaranteed by V2.
   */
  test("Delay Snapshot V2") {
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
    withTempDirAllowFailureInjection { (remoteDir, _) =>
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

          // Creating another DB which will foce a snapshot checkpoint to an older version.
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
          // Checkpointing V2 should ignore the snapshot checkpoint from the previous batch.
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
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
      val rocksdbChangelogCheckpointingConfKey =
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"

      withTempDirAllowFailureInjection { (checkpointDir, injectionState) =>
        injectionState.allowOverwriteInRename = ifAllowRenameOverwrite
        withSQLConf(
          rocksdbChangelogCheckpointingConfKey -> "true",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
          val inputData = MemoryStream[Int]
          val aggregated =
            inputData.toDF()
              .groupBy($"value")
              .agg(count("*"))
              .as[(Int, Long)]

          injectionState.createAtomicDelayCloseRegex = Seq(".*/2_.*changelog")

          val additionalConfs = Map(
            rocksdbChangelogCheckpointingConfKey -> "true",
            SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
            STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fileManagerClassName)
          testStream(aggregated, Update)(
            StartStream(
              checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = additionalConfs),
            AddData(inputData, 3),
            CheckLastBatch((3, 1)),
            AddData(inputData, 3, 2),
            // The second batch should fail to commit because the changelog file is not uploaded
            ExpectFailure[SparkException] { ex =>
              ex.getCause.getMessage.contains("CANNOT_WRITE_STATE_STORE.CANNOT_COMMIT")
            }
          )
          assert(injectionState.delayedStreams.nonEmpty)
          injectionState.delayedStreams.foreach(_.close())
          injectionState.delayedStreams = Seq.empty
          injectionState.createAtomicDelayCloseRegex = Seq.empty

          inputData.addData(3, 1)

          // The query will restart successfully and start at the checkpoint after Batch 1
          testStream(aggregated, Update)(
            StartStream(
              checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = additionalConfs),
            AddData(inputData, 4),
            CheckLastBatch((3, 3), (1, 1), (4, 1)),
            StopStream
          )
        }
      }
    }
  }

  case class FailureConf2(logType: String, checkpointFormatVersion: String) {
    override def toString: String = {
      s"logType = $logType, checkpointFormatVersion = $checkpointFormatVersion"
    }
  }

  // tests to validate the behavior after failures when writing to the commit and offset logs
  Seq(
    FailureConf2("commits", checkpointFormatVersion = "1"),
    FailureConf2("commits", checkpointFormatVersion = "2"),
    FailureConf2("offsets", checkpointFormatVersion = "1"),
    FailureConf2("offsets", checkpointFormatVersion = "2")).foreach { failureConf =>
    test(s"Progress log fails to write $failureConf") {
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
      val rocksdbChangelogCheckpointingConfKey =
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"

      withTempDirAllowFailureInjection { (checkpointDir, injectionState) =>
        withSQLConf(
          rocksdbChangelogCheckpointingConfKey -> "true",
          SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
          val inputData = MemoryStream[Int]
          val aggregated =
            inputData.toDF()
              .groupBy($"value")
              .agg(count("*"))
              .as[(Int, Long)]

          // This should cause the second batch to fail
          injectionState.createAtomicDelayCloseRegex = Seq(s".*/${failureConf.logType}/1")

          val additionalConfs = Map(
            rocksdbChangelogCheckpointingConfKey -> "true",
            SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key ->
              failureConf.checkpointFormatVersion,
            STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fileManagerClassName)

          testStream(aggregated, Update)(
            StartStream(
              checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = additionalConfs),
            AddData(inputData, 3),
            CheckNewAnswer((3, 1)),
            AddData(inputData, 3, 2),
            // We should categorize this error.
            // TODO after the error is categorized, we should check error class
            ExpectFailure[IOException] { _ => () }
          )

          injectionState.createAtomicDelayCloseRegex = Seq.empty

          inputData.addData(3, 1)

          // The query will restart successfully and start at the checkpoint after Batch 1
          testStream(aggregated, Update)(
            StartStream(
              checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = additionalConfs),
            AddData(inputData, 4),
            if (failureConf.logType == "commits") {
              // If the failure is in the commit log, data is already committed.
              // MemorySink isn't an ExactlyOnce sink, so we will see the data from the previous
              // batch. We should see the data from the previous batch and the new data.
              CheckNewAnswer((3, 2), (2, 1), (3, 3), (1, 1), (4, 1))
            } else {
              // If the failure is in the offset log, previous batch didn't run. when the query
              // restarts, it will include all data since the last finished batch.
              CheckNewAnswer((3, 3), (1, 1), (4, 1), (2, 1))

            },
            StopStream
          )
        }
      }
    }
  }

  /**
   * An integrated test to cover this scenario:
   * 1. A batch is running and a snapshot checkpoint is scheduled
   * 2. The batch fails
   * 3. The query restarts
   * 4. The snapshot checkpoint succeeded
   * and validate that the snapshot checkpoint is not used in subsequent query restart.
   */
  test("Previous Maintenance Snapshot Checkpoint Overwrite") {
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
    val rocksdbChangelogCheckpointingConfKey =
      RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"
    withTempDirAllowFailureInjection { (checkpointDir, injectionState) =>
      withSQLConf(
        rocksdbChangelogCheckpointingConfKey -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
        val inputData = MemoryStream[Int]
        val aggregated =
          inputData.toDF()
            .groupBy($"value")
            .agg(count("*"))
            .as[(Int, Long)]

        injectionState.createAtomicDelayCloseRegex = Seq(".*/*zip")

        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = Map(
              rocksdbChangelogCheckpointingConfKey -> "true",
              SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
              SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "20",
              STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fileManagerClassName)),
          AddData(inputData, 3),
          CheckAnswer((3, 1)),
          AddData(inputData, 3, 2),
          AddData(inputData, 3, 1),
          CheckNewAnswer((3, 3), (2, 1), (1, 1)),
          AddData(inputData, 1),
          CheckNewAnswer((1, 2)),
          Execute { _ =>
            // Here we wait for the maintenance thread try to upload zip file and fails from
            // at least one task.
            while (injectionState.delayedStreams.isEmpty) {
              Thread.sleep(1)
            }
            // If we call StoreStore.stop(), or let testStream() call it implicitly, it will
            // cause deadlock. This is a work-around here before we make StateStore.stop() not
            // deadlock.
            StateStore.stopMaintenanceTaskWithoutLock()
          },
          StopStream
        )
        injectionState.createAtomicDelayCloseRegex = Seq.empty

        // Query should still be restarted successfully without losing any data.
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = Map(
              rocksdbChangelogCheckpointingConfKey -> "true",
              SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
              STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fileManagerClassName)),
          AddData(inputData, 1),
          CheckAnswer((1, 3)),
          StopStream
        )
        assert(injectionState.delayedStreams.nonEmpty)
        // This will finish uploading the snapshot checkpoint
        injectionState.delayedStreams.foreach(_.close())
        injectionState.delayedStreams = Seq.empty

        // After previous snapshot checkpoint succeeded, the query can still be restarted correctly.
        testStream(aggregated, Update)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = Map(
              rocksdbChangelogCheckpointingConfKey -> "true",
              SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
              STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fileManagerClassName)),
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
        partitionId = 0,
        eventForwarder = None)
      db.load(version, checkpointId)
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }
}
