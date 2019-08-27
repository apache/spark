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

package org.apache.spark.status

import java.io.BufferedOutputStream
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Status._
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.status.api.v1
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.kvstore.InMemoryStore

case class InMemoryStoreSnapshot(
    store: InMemoryStore,
    eventsNum: Int,
    finished: Boolean) extends Serializable


private[spark] class InMemoryStoreCheckpoint(
    store: InMemoryStore,
    conf: SparkConf,
    listener: AppStatusListener) extends Logging {
  var lastRecordEventsNum: Int = 0
  var finished: Boolean = false

  // used to count the number of processed events in a live AppStatusListener
  private var processedEventsNum = 0
  private val batchSize = conf.get(IMS_CHECKPOINT_BATCH_SIZE)
  private val bufferSize = conf.get(IMS_CHECKPOINT_BUFFER_SIZE).toInt
  private var latch = new CountDownLatch(0)
  @volatile var isDone = true
  // a JavaSerializer used to serialize InMemoryStoreSnapshot
  private val serializer = new JavaSerializer(conf).newInstance()
  private val logBaseDir = Utils.resolveURI(conf.get(EVENT_LOG_DIR).stripSuffix("/"))
  private lazy val ckpPath = getCheckpointPath
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  private val executor = ThreadUtils.newDaemonSingleThreadExecutor(
    "inmemorystore-checkpoint-thread")
  // should be inited before the first time checkpoint
  var appInfo: v1.ApplicationInfo = _

  private val checkpointTask = new Runnable {
    override def run(): Unit = Utils.tryLogNonFatalError(doCheckpoint())
  }

  private def getCheckpointPath: String = {
    val appId = appInfo.id
    val appAttemptId = appInfo.attempts.head.attemptId
    EventLoggingListener.getLogPath(logBaseDir, appId, appAttemptId, None)
  }

  def await(): Unit = latch.await()

  def eventInc(finish: Boolean = false): Unit = {
    processedEventsNum += 1
    val shouldCheckpoint = isDone && (finish || processedEventsNum - lastRecordEventsNum >=
      batchSize)
    if (shouldCheckpoint) {
      // flush to make sure that all processed events' related data have write into InMemoryStore
      listener.flush(listener.update(_, System.nanoTime()))
      latch = new CountDownLatch(1)
      lastRecordEventsNum = processedEventsNum
      if (finish) {
        finished = true
        doCheckpoint()
        executor.shutdown()
      } else {
        executor.submit(checkpointTask)
      }
    }
  }

  private def doCheckpoint(): Unit = {
    try {
      isDone = false
      if (appInfo == null) {
        logWarning("Haven't received event SparkListenerApplicationStart, skip checkpoint")
      } else {
        logInfo(s"Checkpoint InMemoryStore started, eventsNum=$processedEventsNum")
        assert(appInfo != null, "appInfo is null")
        val uri = new Path(ckpPath).toUri
        val ckpFile = new Path(uri.getPath +
          (if (!finished) EventLoggingListener.IN_PROGRESS else "") +
          EventLoggingListener.CHECKPOINT)
        val tmpFile = new Path(ckpFile + ".tmp")
        val fileOut = fileSystem.create(tmpFile)
        val bufferOut = new BufferedOutputStream(fileOut, bufferSize)
        val objOut = serializer.serializeStream(bufferOut)
        val startNs = System.nanoTime()
        objOut.writeObject(InMemoryStoreSnapshot(store, lastRecordEventsNum, finished))
        fileOut.flush()
        objOut.close()
        FileUtil.copy(fileSystem, tmpFile, fileSystem, ckpFile, true, hadoopConf)
        if (finished) {
          val inProgressCkpFile = new Path(uri.getPath + EventLoggingListener.IN_PROGRESS +
            EventLoggingListener.CHECKPOINT)
          if (!fileSystem.delete(inProgressCkpFile, true)) {
            logWarning(s"Failed to delete $inProgressCkpFile")
          }
          if (fileSystem.exists(tmpFile) && !fileSystem.delete(tmpFile, true)) {
            logWarning(s"Failed to delete $tmpFile")
          }
        }
        val finishedNs = System.nanoTime()
        val duration = TimeUnit.NANOSECONDS.toMillis(finishedNs - startNs)
        logInfo(s"Checkpoint InMemoryStore finished, took $duration ms")
      }
    } finally {
      latch.countDown()
      isDone = true
    }
  }
}
