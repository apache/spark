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

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.net.URI
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils


/**
 * A class that enables the async code profiler
 */
private[spark] class ExecutorJVMProfiler(conf: SparkConf, executorId: String) extends Logging {

  private var running = false
  private val enableProfiler = conf.get(EXECUTOR_CODE_PROFILING_ENABLED)
  private val profilerOptions = conf.get(EXECUTOR_CODE_PROFILING_OPTIONS)
  private val profilerOutputDir = conf.get(EXECUTOR_CODE_PROFILING_OUTPUT_DIR)
  private val profilerLocalDir = conf.get(EXECUTOR_CODE_PROFILING_LOCAL_DIR)
  private val writeInterval = conf.get(EXECUTOR_CODE_PROFILING_WRITE_INTERVAL)

  private val startcmd = s"start,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val stopcmd = s"stop,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val dumpcmd = s"dump,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val resumecmd = s"resume,$profilerOptions,file=$profilerLocalDir/profile.jfr"

  private val UPLOAD_SIZE = 8 * 1024 * 1024 // 8 MB
  private var outputStream: FSDataOutputStream = _
  private var inputStream: InputStream = _
  private val dataBuffer = new Array[Byte](UPLOAD_SIZE)
  private var threadpool: ScheduledExecutorService = _
  private var writing: Boolean = false

  val profiler: AsyncProfiler = if (enableProfiler) {
    if (AsyncProfilerLoader.isSupported) {
      AsyncProfilerLoader.load()
    } else {
      logWarning("Executor code profiling is enabled but is not supported for this platform")
      null
    }
  } else {
    null
  }

  def start(): Unit = {
    if (profiler != null && !running) {
      logInfo("Executor code profiling starting.")
      try {
        profiler.execute(startcmd)
      } catch {
        case e: Exception =>
          logWarning("Executor code profiling aborted due to exception: ", e)
          return
      }
      logInfo("Executor code profiling started.")
      running = true
    }
    startWriting()
  }

  /** Stops the profiling and saves output to hdfs location. */
  def stop(): Unit = {
    if (profiler != null && running) {
      profiler.execute(stopcmd)
      logInfo("Code profiler stopped")
      running = false
      finishWriting()
    }
  }

  private def startWriting(): Unit = {
    if (profilerOutputDir.isDefined) {
      val applicationId = conf.getAppId
      val config = SparkHadoopUtil.get.newConfiguration(conf)
      val appName = conf.get("spark.app.name");
      val profilerOutputDirname = profilerOutputDir.get
      val profileOutputFile =
        s"$profilerOutputDirname/$applicationId/profile-$appName-exec-$executorId.jfr"
      val fs = FileSystem.get(new URI(profileOutputFile), config);
      val filenamePath = new Path(profileOutputFile)
      outputStream = fs.create(filenamePath)
      try {
        if (fs.exists(filenamePath)) {
          fs.delete(filenamePath, true)
        }
        logInfo(s"Copying executor profiling file to $profileOutputFile")
        inputStream = new BufferedInputStream(new FileInputStream(s"$profilerLocalDir/profile.jfr"))
        threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("profilerOutputThread")
        threadpool.scheduleWithFixedDelay(new Runnable() {
          override def run(): Unit = writeChunk()
        }, writeInterval, writeInterval,
          TimeUnit.SECONDS)
        writing = true
      } catch {
        case e: Exception =>
          logError("Failed to start code profiler", e)
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

  private def writeChunk(): Unit = {
    if (!writing) {
      return
    }
    try {
      // stop (pause) the profiler, dump the results and then resume. This is not ideal as we miss
      // the events while the file is being dumped, but that is the only way to make sure that
      // the chunk of data we are copying to dfs is in a consistent state.
      profiler.execute(stopcmd)
      profiler.execute(dumpcmd)
      var remaining = inputStream.available()
      profiler.execute(resumecmd)
      while (remaining > 0) {
        val read = inputStream.read(dataBuffer, 0, math.min(remaining, UPLOAD_SIZE))
        outputStream.write(dataBuffer, 0, read)
        remaining -= read
      }
    } catch {
      case e: Exception => logError("Exception occurred while writing profiler output", e)
    }
  }

  private def finishWriting(): Unit = {
    if (profilerOutputDir.isDefined && writing) {
      try {
        // shutdown background writer
        threadpool.shutdown()
        threadpool.awaitTermination(30, TimeUnit.SECONDS)
        // flush remaining data
        writeChunk()
        inputStream.close()
        outputStream.close()
      } catch {
        case e: Exception =>
          logError("Exception in completing profiler output", e)
      }
      writing = false
    }
  }
}
