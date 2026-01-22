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
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{count, udf}
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

  implicit def toStr(bytes: Array[Byte]): String = if (bytes != null) new String(bytes) else null

  def toStr(kv: ByteArrayPair): (String, String) = (toStr(kv.key), toStr(kv.value))

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

            val value = if (ifEnableStateStoreCheckpointIds) {
              // We can write a different value since the files will have different checkpointId.
              "2.2"
            } else {
              // We must write the same value or else checksum verification will fail.
              // This test is only overwriting the state file without overwriting checksum file.
              // Also, since batches are deterministic in checkpoint v1.
              "2.1"
            }
            db.put("version", value)
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

  case class FailureConf3(
      skipCreationIfFileMissingChecksum: Boolean,
      checkpointFormatVersion : String) {
    override def toString: String = {
      s"skipCreationIfFileMissingChecksum = $skipCreationIfFileMissingChecksum, " +
        s"checkpointFormatVersion = $checkpointFormatVersion"
    }
  }

  private def versionsPresent(dir: File, suffix: String): Seq[(Long, Option[String])] = {
    dir.listFiles.filter(_.getName.endsWith(suffix))
    .filter(!_.getName.startsWith("."))
    .map(_.getName.stripSuffix(suffix).split("_"))
    .map {
      case Array(version, uniqueId) => (version.toLong, Some(uniqueId))
      case Array(version) => (version.toLong, None)
    }
    .sorted
    .distinct
    .toSeq
  }

  /**
   * Test that verifies upgrading from checksum disabled to checksum enabled after state files are
   * written but before batch commit completes. The important part of this test is that files are
   * not overwritten if they already exist. When checkpointFormatVersion is 2, we will not run into
   * the checksum verification failure because each batch run uses unique changelog file names.
   *
   * Scenario:
   * 1. Start with checksum verification disabled
   * 2. Run batch 1 successfully (writes 1.changelog without .crc)
   * 3. Start batch 2 - state store commits successfully (writes 2.changelog without .crc) but the
   *    batch fails before the commit is complete (via UDF exception). This leaves 2.changelog on
   *    disk without a corresponding commit log file
   * 4. Restart query with checksum verification enabled and with a query where the changelog file
   *    contents will change from batch 2
   * 5. Run batch 2 again and it succeeds and writes 2.changelog (and 2.changelog.crc if
   *    STREAMING_CHECKPOINT_FILE_CHECKSUM_SKIP_CREATION_IF_FILE_MISSING_CHECKSUM is disabled)
   * 6. Run batch 3 and it succeeds and writes 3.changelog and 3.changelog.crc
   * 7. Query starts with STREAMING_CHECKPOINT_FILE_CHECKSUM_SKIP_CREATION_IF_FILE_MISSING_CHECKSUM
   *    enabled/disabled and the different behavior is shown in the test
   */

  Seq(
    FailureConf3(skipCreationIfFileMissingChecksum = false, checkpointFormatVersion = "1"),
    FailureConf3(skipCreationIfFileMissingChecksum = true, checkpointFormatVersion = "1"),
    FailureConf3(skipCreationIfFileMissingChecksum = false, checkpointFormatVersion = "2"),
    FailureConf3(skipCreationIfFileMissingChecksum = true, checkpointFormatVersion = "2")
  ).foreach { failureConf =>
    test(s"Upgrading from file checksum disabled to enabled " +
      "after state commits without batch commit " + failureConf.toString) {
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fileManagerClassName)
      val rocksdbChangelogCheckpointingConfKey =
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"

      withTempDirAllowFailureInjection { (checkpointDir, injectionState) =>
        var forceTaskFailure = false
          val failUDF = udf((value: Int) => {
          if (forceTaskFailure) {
            // This will fail all close() call to trigger query failures in execution phase.
            throw new RuntimeException("Ingest task failure")
          }
          value
        })

        val inputData = MemoryStream[Int]
        val aggregated =
          inputData.toDF()
            .groupBy($"value")
            .agg(count("*").as("count"))
            // would fail here after writing the changelog file for the agg
            .select(failUDF($"value").as("value"), $"count")
            .as[(Int, Long)]

        val aggregated2 =
          inputData.toDF()
            .select($"value" + 1000 as "value") // This is to make the changelog file different
            .groupBy($"value")
            .agg(count("*").as("count"))
            // would fail here after writing the changelog file for the agg
            .select(failUDF($"value").as("value"), $"count")
            .as[(Int, Long)]

        def getRunConf(checksumEnabled: Boolean) : Map[String, String] = {
          Map(
            rocksdbChangelogCheckpointingConfKey -> "true",
            SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key ->
              failureConf.checkpointFormatVersion,
            SQLConf.STREAMING_CHECKPOINT_FILE_CHECKSUM_ENABLED.key -> checksumEnabled.toString,
            SQLConf.STREAMING_CHECKPOINT_FILE_CHECKSUM_SKIP_CREATION_IF_FILE_MISSING_CHECKSUM.key ->
              failureConf.skipCreationIfFileMissingChecksum.toString,
            STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key -> fileManagerClassName,
            SQLConf.SHUFFLE_PARTITIONS.key -> "1")
        }

        // Verify that the changelog files exists for a version
        def verifyChangelogFileExists(version: Long) : Boolean = {
          versionsPresent(new File(checkpointDir, "state/0/0"), ".changelog").exists {
            case (v, uniqueId) =>
              if (failureConf.checkpointFormatVersion == "1") {
                v == version && uniqueId.isEmpty
              } else {
                v == version && uniqueId.isDefined
              }
          }
        }

        // Verify that the changelog checksum files exists for a version
        def verifyChangelogFileChecksumExists(version: Long) : Boolean = {
          versionsPresent(new File(checkpointDir, "state/0/0"), ".changelog.crc").exists {
            case (v, uniqueId) =>
              if (failureConf.checkpointFormatVersion == "1") {
                v == version && uniqueId.isEmpty
              } else {
                v == version && uniqueId.isDefined
              }
          }
        }

        // First run: file checksum disabled
        val firstRunConfs = getRunConf(checksumEnabled = false)

        testStream(aggregated, Update)(
          StartStream(
            checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = firstRunConfs),
          AddData(inputData, 3),
          CheckLastBatch((3, 1)),
          Execute { _ =>
            forceTaskFailure = true
          },
          AddData(inputData, 3, 2),
          ExpectFailure[SparkException] { ex =>
            ex.getCause.getMessage.contains("FAILED_EXECUTE_UDF")
          }
        )

        // Verify that the changelog file was written
        assert(verifyChangelogFileExists(2))
        // Verify that the changelog file checksum was NOT written since it was disabled
        assert(!verifyChangelogFileChecksumExists(2))

        // Verify that the commit file was written
        assert((new File(checkpointDir, "commits/0")).exists())
        // Verify that the commit file was NOT written
        assert(!(new File(checkpointDir, "commits/1")).exists())

        // Second run: STREAMING_CHECKPOINT_FILE_CHECKSUM_ENABLED enabled with
        // allowOverwriteInRename = false. This simulates an upgrade to a new version where the
        // file checksum is enabled. The allowOverwriteInRename is set to false to test the case
        // when overwriting the changelog file fails. This is to simulate the case where the
        // changelog file is not overwritten but the checksum file is written.
        injectionState.allowOverwriteInRename = false
        forceTaskFailure = false

        val secondRunConfs = getRunConf(checksumEnabled = true)

        inputData.addData(3, 1)

        // The query should restart successfully and handle files without checksums, whether
        // skipCreationIfFileMissingChecksum is enabled or disabled. The problem
        // arises on the load after this run.
        testStream(aggregated2, Update)(
          StartStream(
            checkpointLocation = checkpointDir.getAbsolutePath,
            additionalConfs = secondRunConfs),
          AddData(inputData, 4),
          CheckLastBatch((1003, 2), (1001, 1), (1004, 1)),
          StopStream
        )

        assert(verifyChangelogFileExists(3))
        assert(verifyChangelogFileChecksumExists(3))

        // Verify that the commit files were written
        assert((new File(checkpointDir, "commits/1")).exists())
        assert((new File(checkpointDir, "commits/2")).exists())

        val failureCase =
          !failureConf.skipCreationIfFileMissingChecksum &&
          failureConf.checkpointFormatVersion == "1"

        if (failureCase) {
          assert(verifyChangelogFileChecksumExists(2))

          // The query does not succeed, since we load the old changelog file with the checksum from
          // the new changelog file that did not overwrite the old one. This will lead to a checksum
          // verification failure when we try to load the old changelog file with the checksum from
          // the new changelog file that did not overwrite the old one.
          testStream(aggregated2, Update)(
            StartStream(
              checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = secondRunConfs),
            AddData(inputData, 4),
            ExpectFailure[SparkException] { ex =>
              ex.getMessage.contains("CHECKPOINT_FILE_CHECKSUM_VERIFICATION_FAILED")
              ex.getMessage.contains("2.changelog")
            }
          )

          // Verify that the commit file was not written
          assert(!(new File(checkpointDir, "commits/3")).exists())
        } else {
          if (failureConf.checkpointFormatVersion == "1") {
            // With checkpointFormatVersion = 1, the changelog file checksum should not be written
            assert(!verifyChangelogFileChecksumExists(2))
          } else {
            // With checkpointFormatVersion = 2, the changelog file checksum should be written
            assert(verifyChangelogFileChecksumExists(2))
          }

          // The query should restart successfully
          testStream(aggregated2, Update)(
            StartStream(
              checkpointLocation = checkpointDir.getAbsolutePath,
              additionalConfs = secondRunConfs),
            AddData(inputData, 4),
            CheckLastBatch((1004, 2)),
            StopStream
          )

          // Verify again the 2.changelog file checksum exists or not
          if (failureConf.checkpointFormatVersion == "1") {
            assert(!verifyChangelogFileChecksumExists(2))
          } else {
            assert(verifyChangelogFileChecksumExists(2))
          }

          assert(verifyChangelogFileExists(4))
          assert(verifyChangelogFileChecksumExists(4))
          assert((new File(checkpointDir, "commits/3")).exists())
        }
      }
    }
  }

  /**
   * Test that verifies that when a task is interrupted, the store's rollback() method does not
   * throw an exception and the store can still be used after the rollback.
   */
  test("SPARK-54585: Interrupted task calling rollback does not throw an exception") {
    val hadoopConf = new Configuration()
    hadoopConf.set(
      STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key,
      fileManagerClassName
    )
    withTempDirAllowFailureInjection { (remoteDir, _) =>
      val sqlConf = new SQLConf()
      sqlConf.setConfString("spark.sql.streaming.checkpoint.fileChecksum.enabled", "true")
      val rocksdbChangelogCheckpointingConfKey =
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled"
      sqlConf.setConfString(rocksdbChangelogCheckpointingConfKey, "true")
      val conf = RocksDBConf(StateStoreConf(sqlConf))

      withDB(
        remoteDir.getAbsolutePath,
        version = 0,
        conf = conf,
        hadoopConf = hadoopConf
      ) { db =>
        db.put("key0", "value0")
        val checkpointId1 = commitAndGetCheckpointId(db)

        db.load(1, checkpointId1)
        db.put("key1", "value1")
        val checkpointId2 = commitAndGetCheckpointId(db)

        db.load(2, checkpointId2)
        db.put("key2", "value2")

        // Simulate what happens when a task is killed, the thread's interrupt flag is set.
        // This replicates the scenario where TaskContext.markTaskFailed() is called and
        // the task failure listener invokes RocksDBStateStore.abort() -> rollback().
        Thread.currentThread().interrupt()

        // rollback() should not throw an exception
        db.rollback()

        // Clear the interrupt flag for subsequent operations
        Thread.interrupted()

        // Reload the store and insert a new value
        db.load(2, checkpointId2)
        db.put("key3", "value3")

        // Verify the store has the correct values
        assert(db.iterator().map(toStr).toSet ===
          Set(("key0", "value0"), ("key1", "value1"), ("key3", "value3")))
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
        eventForwarder = None,
        uniqueId = None)
      db.load(version, checkpointId)
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }
}
