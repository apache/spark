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
package org.apache.spark.profiler

import java.io.{BufferedInputStream, FileInputStream, InputStream, IOException}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext.DRIVER_IDENTIFIER
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A class that wraps AsyncProfiler
 */
private[spark] class SparkAsyncProfiler(conf: SparkConf, executorId: String) extends Logging {

  private var running = false
  private val enableProfiler = conf.get(PROFILER_EXECUTOR_ENABLED)
  private val profilerOptions = conf.get(PROFILER_ASYNC_PROFILER_OPTIONS)
  private val profilerDfsDirOpt = conf.get(PROFILER_DFS_DIR)
  private val profilerLocalDir = conf.get(PROFILER_LOCAL_DIR)
  private val writeInterval = conf.get(PROFILER_DFS_WRITE_INTERVAL)

  // app_id and app_attempt_id is unavailable during drvier plugin initialization
  private def getAppId: Option[String] = conf.getOption("spark.app.id")
  private def getAttemptId: Option[String] = conf.getOption("spark.app.attempt.id")

  private val profileFile = if (executorId == DRIVER_IDENTIFIER) {
    s"profile-$executorId.jfr"
  } else {
    s"profile-exec-$executorId.jfr"
  }

  private val startcmd = s"start,$profilerOptions,file=$profilerLocalDir/$profileFile"
  private val stopcmd = s"stop,$profilerOptions,file=$profilerLocalDir/$profileFile"
  private val dumpcmd = s"dump,$profilerOptions,file=$profilerLocalDir/$profileFile"
  private val resumecmd = s"resume,$profilerOptions,file=$profilerLocalDir/$profileFile"

  private val PROFILER_FOLDER_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)
  private val PROFILER_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("660", 8).toShort)
  private val UPLOAD_SIZE = 8 * 1024 * 1024 // 8 MB
  @volatile private var outputStream: FSDataOutputStream = _
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
          logInfo("Profiling started.")
          running = true
          startWriting()
        })
      } catch {
        case e @ (_: IllegalArgumentException | _: IllegalStateException | _: IOException) =>
          logError("Profiling aborted. Exception occurred in async-profiler native code: ", e)
        case e: Exception => logWarning("Profiling aborted due to exception: ", e)
      }
    }
  }

  /** Stops the profiling and saves output to dfs location. */
  def stop(): Unit = {
    if (running) {
      profiler.foreach(p => {
        p.execute(stopcmd)
        logInfo("Profiler stopped")
        running = false
        finishWriting()
      })
    }
  }

  private def requireProfilerBaseDirAsDirectory(fs: FileSystem, profilerDfsDir: String): Unit = {
    if (!fs.getFileStatus(new Path(profilerDfsDir)).isDirectory) {
      throw new IllegalArgumentException(
        s"Profiler DFS base directory $profilerDfsDir is not a directory.")
    }
  }

  private def startWriting(): Unit = {
    profilerDfsDirOpt.foreach { _ =>
      try {
        inputStream = new BufferedInputStream(
          new FileInputStream(s"$profilerLocalDir/$profileFile"))
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
          logError("Failed to start profiler", e)
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

    if (outputStream == null) {
      while (getAppId.isEmpty) {
        logDebug("Waiting for Spark application started")
        Thread.sleep(1000L)
      }
      val baseName = Utils.nameForAppAndAttempt(getAppId.get, getAttemptId)
      val profilerDirForApp = s"${profilerDfsDirOpt.get}/$baseName"
      val profileOutputFile = s"$profilerDirForApp/$profileFile"

      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      val fs = Utils.getHadoopFileSystem(profilerDfsDirOpt.get, hadoopConf)

      requireProfilerBaseDirAsDirectory(fs, profilerDfsDirOpt.get)

      val profilerDirForAppPath = new Path(profilerDirForApp)
      if (!fs.exists(profilerDirForAppPath)) {
        // SPARK-30860: use the class method to avoid the umask causing permission issues
        FileSystem.mkdirs(fs, profilerDirForAppPath, PROFILER_FOLDER_PERMISSIONS)
      }

      logInfo(log"Copying profiling file to ${MDC(PATH, profileOutputFile)}")
      outputStream = FileSystem.create(fs, new Path(profileOutputFile), PROFILER_FILE_PERMISSIONS)
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
        logError("Some profiler output not written. " +
          "Exception occurred in profiler native code: ", e)
      case e: Exception => logError("Some profiler output not written. Unexpected exception: ", e)
    }
  }

  private def finishWriting(): Unit = {
    if (profilerDfsDirOpt.isDefined && writing) {
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
          logWarning("Some profiling output not written. " +
            "Exception occurred while completing profiler output: ", e)
      }
      writing = false
    }
  }
}
