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
package org.apache.spark.executor.profiler

import java.io.{BufferedInputStream, FileInputStream, InputStream, IOException}
import java.net.URI
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.util.ThreadUtils


/**
 * A class that enables the async JVM code profiler
 */
private[spark] class ExecutorJVMProfiler(conf: SparkConf, executorId: String) extends Logging {

  private var running = false
  private val enableProfiler = conf.get(EXECUTOR_PROFILING_ENABLED)
  private val profilerOptions = conf.get(EXECUTOR_PROFILING_OPTIONS)
  private val profilerDfsDir = conf.get(EXECUTOR_PROFILING_DFS_DIR)
  private val profilerLocalDir = conf.get(EXECUTOR_PROFILING_LOCAL_DIR)
  private val writeInterval = conf.get(EXECUTOR_PROFILING_WRITE_INTERVAL)

  private val startcmd = s"start,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val stopcmd = s"stop,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val dumpcmd = s"dump,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val resumecmd = s"resume,$profilerOptions,file=$profilerLocalDir/profile.jfr"

  private val UPLOAD_SIZE = 8 * 1024 * 1024 // 8 MB
  private var outputStream: FSDataOutputStream = _
  private var inputStream: InputStream = _
  private val dataBuffer = new Array[Byte](UPLOAD_SIZE)
  private var threadpool: ScheduledExecutorService = _
  @volatile private var writing: Boolean = false

  val profiler: Option[AsyncProfiler] = {
    Option(
      if (enableProfiler && AsyncProfilerLoader.isSupported) AsyncProfilerLoader.load() else null
    )
  }

  def start(): Unit = {
    if (!running) {
      try {
        profiler.foreach(p => {
          p.execute(startcmd)
          logInfo("Executor JVM profiling started.")
          running = true
          startWriting()
        })
      } catch {
        case e @ (_: IllegalArgumentException | _: IllegalStateException | _: IOException) =>
          logError("JVM profiling aborted. Exception occurred in profiler native code: ", e)
        case e: Exception => logWarning("Executor JVM profiling aborted due to exception: ", e)
      }
    }
  }

  /** Stops the profiling and saves output to dfs location. */
  def stop(): Unit = {
    if (running) {
      profiler.foreach(p => {
        p.execute(stopcmd)
        logInfo("JVM profiler stopped")
        running = false
        finishWriting()
      })
    }
  }

  private def startWriting(): Unit = {
    if (profilerDfsDir.isDefined) {
      val applicationId = try {
        conf.getAppId
      } catch {
        case _: NoSuchElementException => "local-" + System.currentTimeMillis
      }
      val config = SparkHadoopUtil.get.newConfiguration(conf)
      val appName = conf.get("spark.app.name").replace(" ", "-")
      val profilerOutputDirname = profilerDfsDir.get

      val profileOutputFile =
        s"$profilerOutputDirname/$applicationId/profile-$appName-exec-$executorId.jfr"
      val fs = FileSystem.get(new URI(profileOutputFile), config);
      val filenamePath = new Path(profileOutputFile)
      outputStream = fs.create(filenamePath)
      try {
        if (fs.exists(filenamePath)) {
          fs.delete(filenamePath, true)
        }
        logInfo(log"Copying executor profiling file to ${MDC(PATH, profileOutputFile)}")
        inputStream = new BufferedInputStream(new FileInputStream(s"$profilerLocalDir/profile.jfr"))
        threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("profilerOutputThread")
        threadpool.scheduleWithFixedDelay(
          new Runnable() {
            override def run(): Unit = writeChunk(false)
          },
          writeInterval,
          writeInterval,
          TimeUnit.SECONDS)
        writing = true
      } catch {
        case e: Exception =>
          logError("Failed to start JVM profiler", e)
          if (threadpool != null) {
            threadpool.shutdownNow()
          }
          if (inputStream != null) {
            inputStream.close()
          }
          if (outputStream != null) {
            outputStream.close()
          }
      }
    }
  }

  private def writeChunk(lastChunk: Boolean): Unit = {
    if (!writing) {
      return
    }
    try {
      // stop (pause) the profiler, dump the results and then resume. This is not ideal as we miss
      // the events while the file is being dumped, but that is the only way to make sure that
      // the chunk of data we are copying to dfs is in a consistent state.
      profiler.get.execute(stopcmd)
      profiler.get.execute(dumpcmd)
      var remaining = inputStream.available()
      if (!lastChunk) {
        profiler.get.execute(resumecmd)
      }
      while (remaining > 0) {
        val read = inputStream.read(dataBuffer, 0, math.min(remaining, UPLOAD_SIZE))
        outputStream.write(dataBuffer, 0, read)
        remaining -= read
      }
    } catch {
      case e: IOException => logError("Exception occurred while writing some profiler output: ", e)
      case e @ (_: IllegalArgumentException | _: IllegalStateException) =>
        logError("Some profiler output not written." +
          " Exception occurred in profiler native code: ", e)
      case e: Exception => logError("Some profiler output not written. Unexpected exception: ", e)
    }
  }

  private def finishWriting(): Unit = {
    if (profilerDfsDir.isDefined && writing) {
      try {
        // shutdown background writer
        threadpool.shutdown()
        threadpool.awaitTermination(30, TimeUnit.SECONDS)
        // flush remaining data
        writeChunk(true)
        inputStream.close()
        outputStream.close()
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
        case e: IOException =>
          logWarning("Some profiling output not written." +
            "Exception occurred while completing profiler output", e)
      }
      writing = false
    }
  }
}
