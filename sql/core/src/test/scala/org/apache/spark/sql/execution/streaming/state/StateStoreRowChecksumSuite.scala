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

import java.io.{DataInputStream, DataOutputStream}
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.rocksdb.{ReadOptions, RocksDB => NativeRocksDB, WriteOptions}
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Main test for State store row checksum feature. Has both  [[HDFSStateStoreRowChecksumSuite]]
 * and [[RocksDBStateStoreRowChecksumSuite]] as subclasses. Row checksum is also enabled
 * in other tests (State operators, TransformWithState, State data source, RTM etc.)
 * by adding the [[AlsoTestWithStateStoreRowChecksum]] trait.
 * */
abstract class StateStoreRowChecksumSuite extends SharedSparkSession
  with BeforeAndAfter {

  import StateStoreTestsHelper._

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.STATE_STORE_ROW_CHECKSUM_ENABLED.key, true.toString)
      // To avoid file checksum verification since we will be injecting corruption in this suite
      .set(SQLConf.STREAMING_CHECKPOINT_FILE_CHECKSUM_ENABLED.key, false.toString)
  }

  protected def withStateStoreProvider[T <: StateStoreProvider](
      provider: T)(f: T => Unit): Unit = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }

  protected def createProvider: StateStoreProvider

  protected def createInitializedProvider(
      dir: String = Utils.createTempDir().toString,
      opId: Long = 0,
      partition: Int = 0,
      runId: UUID = UUID.randomUUID(),
      keyStateEncoderSpec: KeyStateEncoderSpec = NoPrefixKeyStateEncoderSpec(
        keySchema),
      keySchema: StructType = keySchema,
      valueSchema: StructType = valueSchema,
      sqlConf: SQLConf = SQLConf.get,
      hadoopConf: Configuration = new Configuration): StateStoreProvider = {
    hadoopConf.set(StreamExecution.RUN_ID_KEY, runId.toString)
    val provider = createProvider
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies = false,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  protected def corruptRow(
    provider: StateStoreProvider, store: StateStore, key: UnsafeRow): Unit

  protected def corruptRowInFile(
    provider: StateStoreProvider, version: Long, isSnapshot: Boolean): Unit

  test("Detect local corrupt row") {
    withStateStoreProvider(createInitializedProvider()) { provider =>
      val store = provider.getStore(0)
      put(store, "1", 11, 100)
      put(store, "2", 22, 200)

      // corrupt a row
      corruptRow(provider, store, dataToKeyRow("1", 11))

      assert(get(store, "2", 22).contains(200),
        "failed to get the correct value for the uncorrupted row")

      checkChecksumError(intercept[SparkException] {
        // should throw row checksum exception
        get(store, "1", 11)
      })

      checkChecksumError(intercept[SparkException] {
        // should throw row checksum exception
        store.iterator().foreach(kv => assert(kv.key != null && kv.value != null))
      })

      store.abort()
    }
  }

  protected def corruptRowInFileTestMode: Seq[Boolean]

  corruptRowInFileTestMode.foreach { isSnapshot =>
    test(s"Detect corrupt row in checkpoint file - isSnapshot: $isSnapshot") {
      // We want to generate snapshot per change file
      withSQLConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "0") {
        var version = 0L
        val checkpointDir = Utils.createTempDir().toString

        withStateStoreProvider(createInitializedProvider(checkpointDir)) { provider =>
          val store = provider.getStore(version)
          put(store, "1", 11, 100)
          put(store, "2", 22, 200)
          remove(store, k => k == ("1", 11))
          put(store, "3", 33, 300)
          version = store.commit() // writes change file

          if (isSnapshot) {
            provider.doMaintenance() // writes snapshot
          }
        }

        // We should be able to successfully load the store from checkpoint
        withStateStoreProvider(createInitializedProvider(checkpointDir)) { provider =>
          val store = provider.getStore(version)
          assert(get(store, "1", 11).isEmpty) // the removed row
          assert(get(store, "2", 22).contains(200))
          assert(get(store, "3", 33).contains(300))

          // Now corrupt a row in the checkpoint file
          corruptRowInFile(provider, version, isSnapshot)
          store.abort()
        }

        // Reload the store from checkpoint, should detect the corrupt row
        withStateStoreProvider(createInitializedProvider(checkpointDir)) { provider =>
          checkChecksumError(intercept[SparkException] {
            // should throw row checksum exception
            provider.getStore(version)
          }.getCause.asInstanceOf[SparkException])
        }
      }
    }
  }

  test("Read verification ratio") {
    var version = 0L
    val checkpointDir = Utils.createTempDir().toString
    val numRows = 10

    withStateStoreProvider(createInitializedProvider(checkpointDir)) { provider =>
      val store = provider.getStore(0)
      // add 10 rows
      (1 to numRows).foreach(v => put(store, v.toString, v, v * 100))

      // read the rows
      (1 to numRows).foreach(v => assert(get(store, v.toString, v).nonEmpty))
      val verifier = getReadVerifier(provider, store).get.asInstanceOf[KeyValueChecksumVerifier]
      // Default is to verify every row read
      assertVerifierStats(verifier, expectedRequests = numRows, expectedVerifies = numRows)

      store.iterator().foreach(kv => assert(kv.key != null && kv.value != null))
      assertVerifierStats(verifier, expectedRequests = numRows * 2, expectedVerifies = numRows * 2)

      version = store.commit()
    }

    // Setting to 0 means we will not verify during store read requests
    withSQLConf(SQLConf.STATE_STORE_ROW_CHECKSUM_READ_VERIFICATION_RATIO.key -> "0") {
      withStateStoreProvider(createInitializedProvider(checkpointDir)) { provider =>
        val store = provider.getStore(version)
        (1 to numRows).foreach(v => assert(get(store, v.toString, v).nonEmpty))

        assert(getReadVerifier(provider, store).isEmpty, "Expected no verifier")
        store.abort()
      }
    }

    // Verify every 2 store read requests
    withSQLConf(SQLConf.STATE_STORE_ROW_CHECKSUM_READ_VERIFICATION_RATIO.key -> "2") {
      withStateStoreProvider(createInitializedProvider(checkpointDir)) { provider =>
        val store = provider.getStore(version)

        (1 to numRows).foreach(v => assert(get(store, v.toString, v).nonEmpty))
        val verifier = getReadVerifier(provider, store).get.asInstanceOf[KeyValueChecksumVerifier]
        assertVerifierStats(verifier, expectedRequests = numRows, expectedVerifies = numRows / 2)

        store.iterator().foreach(kv => assert(kv.key != null && kv.value != null))
        assertVerifierStats(verifier, expectedRequests = numRows * 2, expectedVerifies = numRows)

        store.abort()
      }
    }
  }

  protected def checkChecksumError(
      checksumError: SparkException,
      opId: Int = 0,
      partId: Int = 0): Unit = {
    checkError(
      exception = checksumError,
      condition = "STATE_STORE_ROW_CHECKSUM_VERIFICATION_FAILED",
      parameters = Map(
        "stateStoreId" -> (s".*StateStoreId[\\[\\(].*(operatorId|opId)=($opId)" +
          s".*(partitionId|partId)=($partId).*[)\\]]"),
        "expectedChecksum" -> "^-?\\d+$", // integer
        "computedChecksum" -> "^-?\\d+$"), // integer
      matchPVals = true)
  }

  protected def getReadVerifier(
      provider: StateStoreProvider, store: StateStore): Option[KeyValueIntegrityVerifier]

  private def assertVerifierStats(verifier: KeyValueChecksumVerifier,
      expectedRequests: Long, expectedVerifies: Long): Unit = {
    assert(verifier.getNumRequests == expectedRequests)
    assert(verifier.getNumVerified == expectedVerifies)
  }
}

class HDFSStateStoreRowChecksumSuite extends StateStoreRowChecksumSuite with PrivateMethodTester {
  import StateStoreTestsHelper._

  override protected def createProvider: StateStoreProvider = new HDFSBackedStateStoreProvider

  override protected def corruptRow(
      provider: StateStoreProvider, store: StateStore, key: UnsafeRow): Unit = {
    val hdfsProvider = provider.asInstanceOf[HDFSBackedStateStoreProvider]
    val hdfsStore = store.asInstanceOf[hdfsProvider.HDFSBackedStateStore]

    // Access the private hdfs store map
    val mapToUpdateField = PrivateMethod[HDFSBackedStateStoreMap](Symbol("mapToUpdate"))
    val storeMap = hdfsStore invokePrivate mapToUpdateField()
    val mapField = PrivateMethod[HDFSBackedStateStoreMap.MapType](Symbol("map"))
    val map = storeMap invokePrivate mapField()

    val currentValueRow = map.get(key).asInstanceOf[StateStoreRowWithChecksum]
    val currentValue = valueRowToData(currentValueRow.unsafeRow())
    // corrupt the existing value by flipping the last bit
    val corruptValue = currentValue ^ 1

    // update with the corrupt value, but keeping the previous checksum
    map.put(key, StateStoreRowWithChecksum(dataToValueRow(corruptValue), currentValueRow.checksum))
  }

  protected def corruptRowInFile(
      provider: StateStoreProvider, version: Long, isSnapshot: Boolean): Unit = {
    val hdfsProvider = provider.asInstanceOf[HDFSBackedStateStoreProvider]

    val baseDirField = PrivateMethod[Path](Symbol("baseDir"))
    val baseDir = hdfsProvider invokePrivate baseDirField()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new Path(baseDir.toString, fileName)

    val fileManagerMethod = PrivateMethod[CheckpointFileManager](Symbol("fm"))
    val fm = hdfsProvider invokePrivate fileManagerMethod()

    // Read the decompressed file content
    val input = hdfsProvider.decompressStream(fm.open(filePath))
    val currentData = input.readAllBytes()
    input.close()

    // Corrupt the current data by flipping the last bit of the last row we wrote.
    // We typically write an EOF marker (-1) so we are skipping that.
    val byteOffsetToCorrupt = currentData.length - java.lang.Integer.BYTES - 1
    currentData(byteOffsetToCorrupt) = (currentData(byteOffsetToCorrupt) ^ 0x01).toByte

    // Now delete the current file and write a new file with corrupt row
    fm.delete(filePath)
    val output = hdfsProvider.compressStream(fm.createAtomic(filePath, overwriteIfPossible = true))
    output.write(currentData)
    output.close()
  }

  // test both snapshot and delta file
  override protected def corruptRowInFileTestMode: Seq[Boolean] = Seq(true, false)

  protected def getReadVerifier(
      provider: StateStoreProvider, store: StateStore): Option[KeyValueIntegrityVerifier] = {
    val hdfsProvider = provider.asInstanceOf[HDFSBackedStateStoreProvider]
    val hdfsStore = store.asInstanceOf[hdfsProvider.HDFSBackedStateStore]

    // Access the private hdfs store map
    val mapToUpdateField = PrivateMethod[HDFSBackedStateStoreMap](Symbol("mapToUpdate"))
    val storeMap = hdfsStore invokePrivate mapToUpdateField()
    val readVerifierField = PrivateMethod[Option[KeyValueIntegrityVerifier]](
      Symbol("readVerifier"))
    storeMap invokePrivate readVerifierField()
  }

  test("Snapshot upload should fail if corrupt row is detected") {
    // We want to generate snapshot per change file
    withSQLConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "0") {
      withStateStoreProvider(createInitializedProvider()) { provider =>
        val store = provider.getStore(0L)
        put(store, "1", 11, 100)
        put(store, "2", 22, 200)
        put(store, "3", 33, 300)

        // Corrupt the local row, won't affect changelog file since row is already written to it
        corruptRow(provider, store, dataToKeyRow("1", 11))
        store.commit() // writes change file

        checkChecksumError(intercept[SparkException] {
          // Should throw row checksum exception while trying to write snapshot
          // provider.doMaintenance() calls doSnapshot and then swallows exception.
          // Hence, calling doSnapshot directly to avoid that.
          val doSnapshotMethod = PrivateMethod[Unit](Symbol("doSnapshot"))
          provider invokePrivate doSnapshotMethod("maintenance", true)
        })
      }
    }
  }
}

class RocksDBStateStoreRowChecksumSuite extends StateStoreRowChecksumSuite
  with PrivateMethodTester {
  import StateStoreTestsHelper._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Also generate changelog files for RocksDB
      .set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", true.toString)
  }

  override protected def createProvider: StateStoreProvider = new RocksDBStateStoreProvider

  override protected def corruptRow(
      provider: StateStoreProvider, store: StateStore, key: UnsafeRow): Unit = {
    val rocksDBProvider = provider.asInstanceOf[RocksDBStateStoreProvider]
    val dbField = PrivateMethod[NativeRocksDB](Symbol("db"))
    val db = rocksDBProvider.rocksDB invokePrivate dbField()

    val readOptionsField = PrivateMethod[ReadOptions](Symbol("readOptions"))
    val readOptions = rocksDBProvider.rocksDB invokePrivate readOptionsField()
    val writeOptionsField = PrivateMethod[WriteOptions](Symbol("writeOptions"))
    val writeOptions = rocksDBProvider.rocksDB invokePrivate writeOptionsField()

    val encoder = new UnsafeRowDataEncoder(NoPrefixKeyStateEncoderSpec(keySchema), valueSchema)
    val keyBytes = encoder.encodeKey(key)
    val currentValue = db.get(readOptions, keyBytes)

    // corrupt the current value by flipping the last bit
    currentValue(currentValue.length - 1) = (currentValue(currentValue.length - 1) ^ 0x01).toByte
    db.put(writeOptions, keyBytes, currentValue)
  }

  protected def corruptRowInFile(
      provider: StateStoreProvider, version: Long, isSnapshot: Boolean): Unit = {
    assert(!isSnapshot, "Doesn't support corrupting a row in snapshot file")
    val rocksDBProvider = provider.asInstanceOf[RocksDBStateStoreProvider]
    val rocksDBFileManager = rocksDBProvider.rocksDB.fileManager

    val dfsChangelogFileMethod = PrivateMethod[Path](Symbol("dfsChangelogFile"))
    val changelogFilePath = rocksDBFileManager invokePrivate dfsChangelogFileMethod(version, None)

    val fileManagerMethod = PrivateMethod[CheckpointFileManager](Symbol("fm"))
    val fm = rocksDBFileManager invokePrivate fileManagerMethod()

    val codecMethod = PrivateMethod[CompressionCodec](Symbol("codec"))
    val codec = rocksDBFileManager invokePrivate codecMethod()

    // Read the decompressed file content
    val input = new DataInputStream(codec.compressedInputStream(fm.open(changelogFilePath)))
    val currentData = input.readAllBytes()
    input.close()

    // Corrupt the current data by flipping the last bit of the last row we wrote.
    // We typically write an EOF marker (-1) so we are skipping that.
    val byteOffsetToCorrupt = currentData.length - java.lang.Integer.BYTES - 1
    currentData(byteOffsetToCorrupt) = (currentData(byteOffsetToCorrupt) ^ 0x01).toByte

    // Now delete the current file and write a new file with corrupt row
    fm.delete(changelogFilePath)
    val output = new DataOutputStream(
      codec.compressedOutputStream(fm.createAtomic(changelogFilePath, overwriteIfPossible = true)))
    output.write(currentData)
    output.close()
  }

  // test only changelog file since zip file doesn't contain rows
  override protected def corruptRowInFileTestMode: Seq[Boolean] = Seq(false)

  protected def getReadVerifier(
      provider: StateStoreProvider, store: StateStore): Option[KeyValueIntegrityVerifier] = {
    val rocksDBProvider = provider.asInstanceOf[RocksDBStateStoreProvider]
    val readVerifierField = PrivateMethod[Option[KeyValueIntegrityVerifier]](
      Symbol("readVerifier"))
    rocksDBProvider.rocksDB invokePrivate readVerifierField()
  }
}

/**
 * Trait that enables state store row checksum in test sparkConf.
 * Use this to create separate test suites that test with row checksum enabled.
 *
 * Example:
 * {{{
 * class MyTestSuite extends MyBaseTestSuite {
 *   // tests without row checksum
 * }
 *
 * class MyTestSuiteWithRowChecksum extends MyTestSuite with EnableStateStoreRowChecksum {
 *   // inherits all tests from MyTestSuite, but with row checksum enabled
 * }
 * }}}
 */
trait EnableStateStoreRowChecksum extends SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STATE_STORE_ROW_CHECKSUM_ENABLED.key, true.toString)
  }
}
