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

import java.io.File
import java.util.Locale

import org.apache.commons.io.FileUtils
import org.rocksdb._
import org.rocksdb.RocksDB
import org.rocksdb.util.SizeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class RocksDbInstance(keySchema: StructType, valueSchema: StructType, identifier: String)
    extends Logging {

  import RocksDbInstance._
  RocksDB.loadLibrary()

  protected var db: RocksDB = null
  protected var dbPath: String = _
  protected val readOptions: ReadOptions = new ReadOptions()
  protected val writeOptions: WriteOptions = new WriteOptions()
  protected val table_options = new BlockBasedTableConfig
  protected val options: Options = new Options()

  def isOpen(): Boolean = {
    db != null
  }

  def open(path: String, conf: Map[String, String], readOnly: Boolean): Unit = {
    verify(db == null, "Another rocksDb instance is already actve")
    try {
      setOptions(conf)
      db = readOnly match {
        case true =>
          options.setCreateIfMissing(false)
          RocksDB.openReadOnly(options, path)
        case false =>
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
    verify(isOpen(), "Open rocksDb instance before any operation")
    Option(db.get(readOptions, key.getBytes)) match {
      case Some(valueInBytes) =>
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueInBytes, valueInBytes.length)
        value
      case None => null
    }
  }

  def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    verify(isOpen(), "Open rocksDb instance before any operation")
    db.put(key.getBytes, value.getBytes)
  }

  def remove(key: UnsafeRow): Unit = {
    verify(isOpen(), "Open rocksDb instance before any operation")
    db.delete(key.getBytes)
  }

  def commit(backupPath: Option[String] = None): Unit = {
    backupPath.foreach(f => createCheckpoint(db, f))
  }

  def abort: Unit = {
    // no-op
  }

  def close(): Unit = {
    readOptions.close()
    writeOptions.close()
    logDebug("Closing the db")
    db.close()
    db = null
  }

  def iterator(closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {
    verify(isOpen(), "Open rocksDb instance before any operation")
    Option(db.getSnapshot) match {
      case Some(snapshot) =>
        logDebug(s"Inside rockdDB iterator function")
        var snapshotReadOptions: ReadOptions = new ReadOptions().setSnapshot(snapshot)
        val itr = db.newIterator(snapshotReadOptions)
        createUnsafeRowPairIterator(itr, snapshotReadOptions, closeDbOnCompletion)
      case None =>
        Iterator.empty
    }
  }

  protected def createUnsafeRowPairIterator(
      itr: RocksIterator,
      itrReadOptions: ReadOptions,
      closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {

    itr.seekToFirst()

    new Iterator[UnsafeRowPair] {
      override def hasNext: Boolean = {
        if (itr.isValid) {
          true
        } else {
          itrReadOptions.close()
          if (closeDbOnCompletion) {
            close()
          }
          logDebug(s"read from DB completed")
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

  def printStats: Unit = {
    verify(isOpen(), "Open rocksDb instance before any operation")
    try {
      val stats = db.getProperty("rocksdb.stats")
      logInfo(s"Stats = $stats")
    } catch {
      case e: Exception =>
        logWarning("Exception while getting stats")
    }
  }

  def setOptions(conf: Map[String, String]): Unit = {

    // Read options
    readOptions.setFillCache(false)

    // Write options
    writeOptions.setSync(false)
    writeOptions.setDisableWAL(true)

    val dataBlockSize = conf
      .getOrElse(
        "spark.sql.streaming.stateStore.rocksDb.blockSizeInKB".toLowerCase(Locale.ROOT),
        "64")
      .toInt

    val metadataBlockSize = conf
      .getOrElse(
        "spark.sql.streaming.stateStore.rocksDb.metadataBlockSizeInKB".toLowerCase(Locale.ROOT),
        "4")
      .toInt

    // Table configs
    // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
    table_options
      .setBlockSize(dataBlockSize)
      .setBlockSizeDeviation(5)
      .setMetadataBlockSize(metadataBlockSize)
      .setFilterPolicy(new BloomFilter(10, false))
      .setPartitionFilters(true)
      .setIndexType(IndexType.kTwoLevelIndexSearch)
      .setBlockCache(lRUCache)
      .setCacheIndexAndFilterBlocks(true)
      .setPinTopLevelIndexAndFilter(true)
      .setCacheIndexAndFilterBlocksWithHighPriority(true)
      .setPinL0FilterAndIndexBlocksInCache(true)
      .setFormatVersion(4) // https://rocksdb.org/blog/2019/03/08/format-version-4.html
      .setIndexBlockRestartInterval(16)

    var bufferNumber = conf
      .getOrElse(
        "spark.sql.streaming.stateStore.rocksDb.bufferNumber".toLowerCase(Locale.ROOT),
        "5")
      .toInt

    bufferNumber = Math.max(bufferNumber, 3)

    val bufferNumberToMaintain = Math.max(bufferNumber - 2, 3)

    logInfo(
      s"Using Max Buffer Name = $bufferNumber & " +
        s"max buffer number to maintain = $bufferNumberToMaintain")

    // DB Options
    options
      .setCreateIfMissing(true)
      .setMaxWriteBufferNumber(bufferNumber)
      .setMaxWriteBufferNumberToMaintain(bufferNumberToMaintain)
      .setMaxBackgroundCompactions(4)
      .setMaxBackgroundFlushes(2)
      .setMaxOpenFiles(-1)
      .setMaxFileOpeningThreads(4)
      .setWriteBufferSize(256 * SizeUnit.MB)
      .setTargetFileSizeBase(256 * SizeUnit.MB)
      .setLevelZeroFileNumCompactionTrigger(10)
      .setLevelZeroSlowdownWritesTrigger(20)
      .setLevelZeroStopWritesTrigger(40)
      .setMaxBytesForLevelBase(2 * SizeUnit.GB)
      .setTableFormatConfig(table_options)

  }

  def createCheckpoint(rocksDb: RocksDB, dir: String): Unit = {
    verify(isOpen(), "Open rocksDb instance before any operation")
    val (result, elapsedMs) = Utils.timeTakenMs {
      val c = Checkpoint.create(rocksDb)
      val f: File = new File(dir)
      if (f.exists()) {
        FileUtils.deleteDirectory(f)
      }
      c.createCheckpoint(dir)
      c.close()
    }
    logDebug(s"Creating createCheckpoint at $dir took $elapsedMs ms.")
  }

  def createBackup(dir: String): Unit = {
    verify(isOpen(), "Open rocksDb instance before any operation")
    val (result, elapsedMs) = Utils.timeTakenMs {
      val backupableDBOptions = new BackupableDBOptions(dir)
      backupableDBOptions.setDestroyOldData(true)
      val env: Env = Env.getDefault
      env.setBackgroundThreads(2)
      val be = BackupEngine.open(env, backupableDBOptions)
      be.createNewBackup(db, true) //
      backupableDBOptions.close()
      env.close()
      be.close()
    }
    logInfo(s"Creating backup at $dir takes $elapsedMs ms.")
  }
}

class OptimisticTransactionDbInstance(
    keySchema: StructType,
    valueSchema: StructType,
    identifier: String)
    extends RocksDbInstance(keySchema: StructType, valueSchema: StructType, identifier: String) {

  import RocksDbInstance._
  RocksDB.loadLibrary()

  var otdb: OptimisticTransactionDB = null
  var txn: Transaction = null

  override def isOpen(): Boolean = {
    otdb != null
  }

  def open(path: String, conf: Map[String, String]): Unit = {
    open(path, conf, false)
  }

  override def open(path: String, conf: Map[String, String], readOnly: Boolean): Unit = {
    verify(otdb == null, "Another OptimisticTransactionDbInstance instance is already actve")
    verify(readOnly == false, "Cannot open OptimisticTransactionDbInstance in Readonly mode")
    try {
      setOptions(conf)
      options.setCreateIfMissing(true)
      otdb = OptimisticTransactionDB.open(options, path)
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
    verify(isOpen(), "Open OptimisticTransactionDbInstance before performing any operation")
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
    verify(txn != null, "Start Transaction before inserting any key")
    txn.put(key.getBytes, value.getBytes)
  }

  override def remove(key: UnsafeRow): Unit = {
    verify(txn != null, "Start Transaction before deleting any key")
    txn.delete(key.getBytes)
  }

  override def get(key: UnsafeRow): UnsafeRow = {
    verify(txn != null, "Start Transaction before fetching any key-value")
    Option(txn.get(readOptions, key.getBytes)) match {
      case Some(valueInBytes) =>
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueInBytes, valueInBytes.length)
        value
      case None =>
        null
    }
  }

  override def commit(backupPath: Option[String] = None): Unit = {
    verify(txn != null, "Start Transaction before fetching any key-value")
    // printTrxStats
    try {
      val file = new File(dbPath, identifier.toUpperCase(Locale.ROOT))
      file.createNewFile()
      txn.commit()
      txn.close()
      txn = null
      backupPath.foreach(f => createCheckpoint(otdb.asInstanceOf[RocksDB], f))
    } catch {
      case e: Exception =>
        log.error(s"Unable to commit the transactions. Error message = ${e.getMessage}")
        throw e
    }
  }

  def printTrxStats(): Unit = {
    verify(txn != null, "No open Transaction")
    logInfo(s"""
         | deletes = ${txn.getNumDeletes}
         | numKeys = ${txn.getNumKeys}
         | puts =  ${txn.getNumPuts}
         | time =  ${txn.getElapsedTime}
       """.stripMargin)
  }

  override def abort(): Unit = {
    verify(txn != null, "No Transaction to abort")
    txn.rollbackToSavePoint()
    txn.close()
    txn = null
  }

  override def close(): Unit = {
    verify(isOpen(), "No DB to close")
    readOptions.close()
    writeOptions.close()
    logDebug("Closing the transaction db")
    otdb.close()
    otdb = null
  }

  override def iterator(closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {
    verify(txn != null, "Transaction is not set")
    verify(
      closeDbOnCompletion == false,
      "Cannot close a DB without aborting/commiting the transactions")
    val readOptions = new ReadOptions()
    val itr: RocksIterator = txn.getIterator(readOptions)
    Option(itr) match {
      case Some(i) =>
        logDebug(s"creating iterator from transaction DB")
        createUnsafeRowPairIterator(i, readOptions, false)
      case None =>
        Iterator.empty
    }
  }

}

object RocksDbInstance {

  RocksDB.loadLibrary()

  private val destroyOptions: Options = new Options()

  val lRUCache = new LRUCache(1024 * 1024 * 1024, 6, false, 0.05)

  def destroyDB(path: String): Unit = {
    val f: File = new File(path)
    if (f.exists()) {
      RocksDB.destroyDB(path, destroyOptions)
      FileUtils.deleteDirectory(f)
    }
  }

  def restoreFromBackup(backupDir: String, dbDir: String): Unit = {
    val (result, elapsedMs) = Utils.timeTakenMs {
      val backupableDBOptions = new BackupableDBOptions(backupDir)
      val be = BackupEngine.open(Env.getDefault, backupableDBOptions)
      val restoreOptions = new RestoreOptions(false)
      be.restoreDbFromLatestBackup(dbDir, dbDir, restoreOptions)
      restoreOptions.close()
      backupableDBOptions.close()
      be.close()
    }
  }

  def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

}
