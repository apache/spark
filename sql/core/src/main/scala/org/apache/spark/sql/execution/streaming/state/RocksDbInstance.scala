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

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.rocksdb._
import org.rocksdb.RocksDB

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class RocksDbInstance(keySchema: StructType, valueSchema: StructType, version: String)
    extends Logging {

  import RocksDbInstance._
  RocksDB.loadLibrary()

  protected var db: RocksDB = null
  protected var dbPath: String = _
  protected val readOptions: ReadOptions = new ReadOptions()
  protected val writeOptions: WriteOptions = new WriteOptions()
  protected val table_options = new BlockBasedTableConfig
  protected val options: Options = new Options()

  private def isOpen(): Boolean = {
    db != null
  }

  def open(path: String, readOnly: Boolean): Unit = {
    require(db == null, "Another rocksDb instance is already active")
    try {
      setOptions
      db = if (readOnly) {
        options.setCreateIfMissing(false)
        RocksDB.openReadOnly(options, path)
      } else {
        options.setCreateIfMissing(true)
        RocksDB.open(options, path)
      }
      dbPath = path
    } catch {
      case e: Throwable =>
        throw new IllegalStateException(
          s"Error while creating rocksDb instance ${e.getMessage}",
          e)
    }
  }

  def get(key: UnsafeRow): UnsafeRow = {
    require(isOpen(), "Open rocksDb instance before any operation")
    Option(db.get(readOptions, key.getBytes)) match {
      case Some(valueInBytes) =>
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueInBytes, valueInBytes.length)
        value
      case None => null
    }
  }

  def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    db.put(key.getBytes, value.getBytes)
  }

  def remove(key: UnsafeRow): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    db.delete(key.getBytes)
  }

  def commit(checkPointPath: Option[String] = None): Unit = {
    checkPointPath.foreach(f => createCheckpoint(db, f))
  }

  def abort: Unit = {
    // no-op
  }

  def close(): Unit = {
    logDebug("Closing the db")
    try {
      db.close()
    } finally {
      db = null
      options.close()
      readOptions.close()
      writeOptions.close()
    }
  }

  def iterator(closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {
    require(isOpen(), "Open rocksDb instance before any operation")
    Option(db.getSnapshot) match {
      case Some(snapshot) =>
        var snapshotReadOptions: ReadOptions =
          new ReadOptions().setSnapshot(snapshot).setFillCache(false)
        val itr = db.newIterator(snapshotReadOptions)
        createUnsafeRowPairIterator(itr, snapshotReadOptions, snapshot, closeDbOnCompletion)
      case None =>
        Iterator.empty
    }
  }

  protected def createUnsafeRowPairIterator(
      itr: RocksIterator,
      itrReadOptions: ReadOptions,
      snapshot: Snapshot,
      closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {

    itr.seekToFirst()

    new Iterator[UnsafeRowPair] {
      @volatile var isClosed = false
      override def hasNext: Boolean = {
        if (!isClosed && itr.isValid) {
          true
        } else {
          if (!isClosed) {
            isClosed = true
            itrReadOptions.close()
            db.releaseSnapshot(snapshot)
            if (closeDbOnCompletion) {
              close()
            }
            itr.close()
            logDebug(s"read from DB completed")
          }
          false
        }
      }

      override def next(): UnsafeRowPair = {
        val keyBytes = itr.key
        val key = new UnsafeRow(keySchema.fields.length)
        key.pointTo(keyBytes, keyBytes.length)
        val valueBytes = itr.value
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueBytes, valueBytes.length)
        itr.next()
        new UnsafeRowPair(key, value)
      }
    }
  }

  protected def printMemoryStats(db: RocksDB): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    val usage = MemoryUtil
      .getApproximateMemoryUsageByType(
        List(db).asJava,
        Set(rocksDbLRUCache.asInstanceOf[Cache]).asJava)
      .asScala
    val numKeys = db.getProperty(db.getDefaultColumnFamily, "rocksdb.estimate-num-keys")
    logDebug(s"""
                | rocksdb.estimate-num-keys = $numKeys
                | ApproximateMemoryUsageByType = ${usage.toString}
                | """.stripMargin)
  }

  protected def printStats: Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    try {
      val stats = db.getProperty("rocksdb.stats")
      logInfo(s"Stats = $stats")
    } catch {
      case e: Exception =>
        logWarning("Exception while getting stats")
    }
  }

  private val dataBlockSize = RocksDbStateStoreConf.blockSizeInKB
  private val memTableMemoryBudget = RocksDbStateStoreConf.memtableBudgetInMB
  private val enableStats = RocksDbStateStoreConf.enableStats

  protected def setOptions(): Unit = {

    // Read options
    readOptions.setFillCache(true)

    // Write options
    writeOptions.setSync(false)
    writeOptions.setDisableWAL(true)

    /*
      Table configs
      Use Partitioned Index Filters
      https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
      Use format Verion = 4
      https://rocksdb.org/blog/2019/03/08/format-version-4.html
     */
    table_options
      .setBlockSize(dataBlockSize * 1024)
      .setFormatVersion(4)
      .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
      .setBlockCache(rocksDbLRUCache)
      .setFilterPolicy(new BloomFilter(10, false))
      .setPinTopLevelIndexAndFilter(false) // Dont pin anything in cache
      .setIndexType(IndexType.kTwoLevelIndexSearch)
      .setPartitionFilters(true)

    options
      .setTableFormatConfig(table_options)
      .optimizeLevelStyleCompaction(memTableMemoryBudget * 1024 * 1024)
      .setBytesPerSync(1048576)
      .setMaxOpenFiles(5000)
      .setIncreaseParallelism(4)

    if (enableStats) {
      options
        .setStatistics(new Statistics())
        .setStatsDumpPeriodSec(30)
    }
  }

  protected def createCheckpoint(rocksDb: RocksDB, dir: String): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    val (result, elapsedMs) = Utils.timeTakenMs {
      val c = Checkpoint.create(rocksDb)
      val f: File = new File(dir)
      if (f.exists()) {
        FileUtils.deleteDirectory(f)
      }
      c.createCheckpoint(dir)
      c.close()
    }
    logInfo(s"Creating Checkpoint at $dir took $elapsedMs ms.")
  }
}

class OptimisticTransactionDbInstance(
    keySchema: StructType,
    valueSchema: StructType,
    version: String)
    extends RocksDbInstance(keySchema: StructType, valueSchema: StructType, version: String) {
  import RocksDbInstance._
  RocksDB.loadLibrary()

  private var otdb: OptimisticTransactionDB = null
  private var txn: Transaction = null

  private def isOpen(): Boolean = {
    otdb != null
  }

  def open(path: String): Unit = {
    open(path, false)
  }

  override def open(path: String, readOnly: Boolean): Unit = {
    require(otdb == null, "Another OptimisticTransactionDbInstance instance is already active")
    require(readOnly == false, "Cannot open OptimisticTransactionDbInstance in Readonly mode")
    try {
      setOptions()
      options.setCreateIfMissing(true)
      otdb = OptimisticTransactionDB.open(options, path)
      db = otdb.getBaseDB
      dbPath = path
    } catch {
      case e: Throwable =>
        throw new IllegalStateException(
          s"Error while creating OptimisticTransactionDb instance" +
            s" ${e.getMessage}",
          e)
    }
  }

  def startTransactions(): Unit = {
    require(isOpen(), "Open OptimisticTransactionDbInstance before performing any operation")
    Option(txn) match {
      case None =>
        val optimisticTransactionOptions = new OptimisticTransactionOptions()
        txn = otdb.beginTransaction(writeOptions, optimisticTransactionOptions)
        txn.setSavePoint()
      case Some(x) =>
        throw new IllegalStateException(s"Already started a transaction")
    }
  }

  override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    require(txn != null, "Start Transaction before inserting any key")
    txn.put(key.getBytes, value.getBytes)
  }

  override def remove(key: UnsafeRow): Unit = {
    require(txn != null, "Start Transaction before deleting any key")
    txn.delete(key.getBytes)
  }

  override def get(key: UnsafeRow): UnsafeRow = {
    require(txn != null, "Start Transaction before fetching any key-value")
    Option(txn.get(readOptions, key.getBytes)) match {
      case Some(valueInBytes) =>
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueInBytes, valueInBytes.length)
        value
      case None =>
        null
    }
  }

  override def commit(checkPointPath: Option[String] = None): Unit = {
    require(txn != null, "Start Transaction before fetching any key-value")
    try {
      txn.commit()
      txn.close()
      updateVersionInCommitFile()
      checkPointPath.foreach(f => createCheckpoint(otdb.asInstanceOf[RocksDB], f))
    } catch {
      case e: Exception =>
        log.error(s"Unable to commit the transactions. Error message = ${e.getMessage}")
        throw e
    } finally {
      txn = null
    }
  }

  override def abort(): Unit = {
    require(txn != null, "No Transaction to abort")
    txn.rollbackToSavePoint()
    txn.close()
    txn = null
  }

  override def close(): Unit = {
    require(isOpen(), "No DB to close")
    require(txn == null, "Transaction should be closed before closing the DB connection")
    printMemoryStats(otdb.asInstanceOf[RocksDB])
    logDebug("Closing the transaction db")
    try {
      otdb.close()
      db.close()
      otdb = null
      db = null
    } finally {
      options.close()
      readOptions.close()
      writeOptions.close()
    }
  }

  override def iterator(closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {
    require(txn != null, "Transaction is not set")
    require(
      closeDbOnCompletion == false,
      "Cannot close a DB without aborting/committing the transactions")
    val snapshot = db.getSnapshot
    val readOptions = new ReadOptions()
      .setSnapshot(snapshot)
      .setFillCache(false) // for range lookup, we should not fill cache
    val itr: RocksIterator = txn.getIterator(readOptions)
    Option(itr) match {
      case Some(i) =>
        logDebug(s"creating iterator from a transactional DB")
        createUnsafeRowPairIterator(i, readOptions, snapshot, false)
      case None =>
        Iterator.empty
    }
  }

  def getApproxEntriesInDb(): Long = {
    require(isOpen(), "No DB to find Database Entries")
    otdb.getProperty("rocksdb.estimate-num-keys").toLong
  }

  protected def updateVersionInCommitFile(): Unit = {
    val file = new File(dbPath, COMMIT_FILE_NAME)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(version.toString)
    bw.close()
  }

}

object RocksDbInstance {

  RocksDB.loadLibrary()

  val COMMIT_FILE_NAME = "commit"

  lazy val rocksDbLRUCache = new LRUCache(RocksDbStateStoreConf.cacheSize * 1024 * 1024, 6, false)

  def destroyDB(path: String): Unit = {
    val f: File = new File(path)
    val destroyOptions: Options = new Options()
    if (f.exists()) {
      RocksDB.destroyDB(path, destroyOptions)
      FileUtils.deleteDirectory(f)
    }
  }

}
