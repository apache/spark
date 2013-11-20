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

package org.apache.spark.streaming

import java.io._
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.MetadataCleaner


private[streaming]
class Checkpoint(@transient ssc: StreamingContext, val checkpointTime: Time)
  extends Logging with Serializable {
  val master = ssc.sc.master
  val framework = ssc.sc.appName
  val sparkHome = ssc.sc.sparkHome
  val jars = ssc.sc.jars
  val environment = ssc.sc.environment
  val graph = ssc.graph
  val checkpointDir = ssc.checkpointDir
  val checkpointDuration = ssc.checkpointDuration
  val pendingTimes = ssc.scheduler.jobManager.getPendingTimes()
  val delaySeconds = MetadataCleaner.getDelaySeconds

  def validate() {
    assert(master != null, "Checkpoint.master is null")
    assert(framework != null, "Checkpoint.framework is null")
    assert(graph != null, "Checkpoint.graph is null")
    assert(checkpointTime != null, "Checkpoint.checkpointTime is null")
    logInfo("Checkpoint for time " + checkpointTime + " validated")
  }
}


/**
 * Convenience class to speed up the writing of graph checkpoint to file
 */
private[streaming]
class CheckpointWriter(checkpointDir: String) extends Logging {
  val file = new Path(checkpointDir, "graph")
  // The file to which we actually write - and then "move" to file.
  private val writeFile = new Path(file.getParent, file.getName + ".next")
  private val bakFile = new Path(file.getParent, file.getName + ".bk")

  private var stopped = false

  val conf = new Configuration()
  var fs = file.getFileSystem(conf)
  val maxAttempts = 3
  val executor = Executors.newFixedThreadPool(1)

  private val compressionCodec = CompressionCodec.createCodec()

  // Removed code which validates whether there is only one CheckpointWriter per path 'file' since 
  // I did not notice any errors - reintroduce it ?

  class CheckpointWriteHandler(checkpointTime: Time, bytes: Array[Byte]) extends Runnable {
    def run() {
      var attempts = 0
      val startTime = System.currentTimeMillis()
      while (attempts < maxAttempts) {
        attempts += 1
        try {
          logDebug("Saving checkpoint for time " + checkpointTime + " to file '" + file + "'")
          // This is inherently thread unsafe .. so alleviating it by writing to '.new' and then doing moves : which should be pretty fast.
          val fos = fs.create(writeFile)
          fos.write(bytes)
          fos.close()
          if (fs.exists(file) && fs.rename(file, bakFile)) {
            logDebug("Moved existing checkpoint file to " + bakFile)
          }
          // paranoia
          fs.delete(file, false)
          fs.rename(writeFile, file)

          val finishTime = System.currentTimeMillis()
          logInfo("Checkpoint for time " + checkpointTime + " saved to file '" + file +
            "', took " + bytes.length + " bytes and " + (finishTime - startTime) + " milliseconds")
          return
        } catch {
          case ioe: IOException =>
            logWarning("Error writing checkpoint to file in " + attempts + " attempts", ioe)
        }
      }
      logError("Could not write checkpoint for time " + checkpointTime + " to file '" + file + "'")
    }
  }

  def write(checkpoint: Checkpoint) {
    val bos = new ByteArrayOutputStream()
    val zos = compressionCodec.compressedOutputStream(bos)
    val oos = new ObjectOutputStream(zos)
    oos.writeObject(checkpoint)
    oos.close()
    bos.close()
    try {
      executor.execute(new CheckpointWriteHandler(checkpoint.checkpointTime, bos.toByteArray))
    } catch {
      case rej: RejectedExecutionException =>
        logError("Could not submit checkpoint task to the thread pool executor", rej)
    }
  }

  def stop() {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    executor.shutdown()
    val startTime = System.currentTimeMillis()
    val terminated = executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)
    val endTime = System.currentTimeMillis()
    logInfo("CheckpointWriter executor terminated ? " + terminated + ", waited for " + (endTime - startTime) + " ms.")
  }
}


private[streaming]
object CheckpointReader extends Logging {

  def read(path: String): Checkpoint = {
    val fs = new Path(path).getFileSystem(new Configuration())
    val attempts = Seq(new Path(path, "graph"), new Path(path, "graph.bk"), new Path(path), new Path(path + ".bk"))

    val compressionCodec = CompressionCodec.createCodec()

    attempts.foreach(file => {
      if (fs.exists(file)) {
        logInfo("Attempting to load checkpoint from file '" + file + "'")
        try {
          val fis = fs.open(file)
          // ObjectInputStream uses the last defined user-defined class loader in the stack
          // to find classes, which maybe the wrong class loader. Hence, a inherited version
          // of ObjectInputStream is used to explicitly use the current thread's default class
          // loader to find and load classes. This is a well know Java issue and has popped up
          // in other places (e.g., http://jira.codehaus.org/browse/GROOVY-1627)
          val zis = compressionCodec.compressedInputStream(fis)
          val ois = new ObjectInputStreamWithLoader(zis, Thread.currentThread().getContextClassLoader)
          val cp = ois.readObject.asInstanceOf[Checkpoint]
          ois.close()
          fs.close()
          cp.validate()
          logInfo("Checkpoint successfully loaded from file '" + file + "'")
          logInfo("Checkpoint was generated at time " + cp.checkpointTime)
          return cp
        } catch {
          case e: Exception =>
            logError("Error loading checkpoint from file '" + file + "'", e)
        }
      } else {
        logWarning("Could not read checkpoint from file '" + file + "' as it does not exist")
      }

    })
    throw new Exception("Could not read checkpoint from path '" + path + "'")
  }
}

private[streaming]
class ObjectInputStreamWithLoader(inputStream_ : InputStream, loader: ClassLoader)
  extends ObjectInputStream(inputStream_) {

  override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      return loader.loadClass(desc.getName())
    } catch {
      case e: Exception =>
    }
    return super.resolveClass(desc)
  }
}
