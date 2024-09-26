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

import org.rocksdb.{RocksDB => NativeRocksDB}

import org.apache.spark.util.UninterruptibleThread

/**
 * A wrapper for RocksDB library loading using an uninterruptible thread, as the native RocksDB
 * code will throw an error when interrupted.
 */
object RocksDBLoader extends StateStoreThreadAwareLogging {
  /**
   * Keep tracks of the exception thrown from the loading thread, if any.
   */
  private var exception: Option[Throwable] = null

  private val loadLibraryThread = new UninterruptibleThread("RocksDBLoader") {
    override def run(): Unit = {
      try {
        runUninterruptibly {
          NativeRocksDB.loadLibrary()
          exception = None
        }
      } catch {
        case e: Throwable =>
          exception = Some(e)
      }
    }
  }

  def loadLibrary(): Unit = synchronized {
    if (exception == null) {
      // SPARK-39847: if a task thread is interrupted while blocking in this loadLibrary()
      // call then a second task thread might start a loadLibrary() call while the first
      // call's loadLibraryThread is still running. Checking loadLibraryThread's state here
      // ensures that the second loadLibrary() call will wait for the original call's
      // loadLibraryThread to complete. If we didn't have this call then the second
      // loadLibraryCall() would call start() on an already-started thread, causing a
      // java.lang.IllegalThreadStateException error.
      if (loadLibraryThread.getState == Thread.State.NEW) {
        loadLibraryThread.start()
      }
      logInfo("RocksDB library loading thread started")
      loadLibraryThread.join()
      exception.foreach(throw _)
      logInfo("RocksDB library loading thread finished successfully")
    } else {
      exception.foreach(throw _)
    }
  }
}
