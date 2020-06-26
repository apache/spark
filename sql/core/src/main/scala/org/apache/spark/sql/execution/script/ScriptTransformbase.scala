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

package org.apache.spark.sql.execution.script

import java.io.OutputStream
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{CircularBuffer, SerializableConfiguration, Utils}

trait ScriptTransformBase extends UnaryExecNode {
  override def doExecute(): RDD[InternalRow] = {
    val broadcastedHadoopConf =
      new SerializableConfiguration(sqlContext.sessionState.newHadoopConf())

    child.execute().mapPartitions { iter =>
      if (iter.hasNext) {
        val proj = UnsafeProjection.create(schema)
        processIterator(iter, broadcastedHadoopConf.value).map(proj)
      } else {
        // If the input iterator has no rows then do not launch the external script.
        Iterator.empty
      }
    }
  }

  def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration) : Iterator[InternalRow]


  protected def checkFailureAndPropagate(
      writerThread: ScriptTransformationWriterThreadBase,
      cause: Throwable = null,
      proc: Process,
      stderrBuffer: CircularBuffer): Unit = {
    if (writerThread.exception.isDefined) {
      throw writerThread.exception.get
    }

    // There can be a lag between reader read EOF and the process termination.
    // If the script fails to startup, this kind of error may be missed.
    // So explicitly waiting for the process termination.
    val timeout = conf.getConf(SQLConf.SCRIPT_TRANSFORMATION_EXIT_TIMEOUT)
    val exitRes = proc.waitFor(timeout, TimeUnit.SECONDS)
    if (!exitRes) {
      log.warn(s"Transformation script process exits timeout in $timeout seconds")
    }

    if (!proc.isAlive) {
      val exitCode = proc.exitValue()
      if (exitCode != 0) {
        logError(stderrBuffer.toString) // log the stderr circular buffer
        throw new SparkException(s"Subprocess exited with status $exitCode. " +
          s"Error: ${stderrBuffer.toString}", cause)
      }
    }
  }
}

abstract class ScriptTransformationWriterThreadBase(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration) extends Thread with Logging {
  @volatile protected var _exception: Throwable = null

  /** Contains the exception thrown while writing the parent iterator to the external process. */
  def exception: Option[Throwable] = Option(_exception)

  protected def processRows(): Unit

  override def run(): Unit = Utils.logUncaughtExceptions {
    TaskContext.setTaskContext(taskContext)

    // We can't use Utils.tryWithSafeFinally here because we also need a `catch` block, so
    // let's use a variable to record whether the `finally` block was hit due to an exception
    var threwException: Boolean = true
    try {
      processRows()
      threwException = false
    } catch {
      // SPARK-25158 Exception should not be thrown again, otherwise it will be captured by
      // SparkUncaughtExceptionHandler, then Executor will exit because of this Uncaught Exception,
      // so pass the exception to `ScriptTransformationExec` is enough.
      case t: Throwable =>
        // An error occurred while writing input, so kill the child process. According to the
        // Javadoc this call will not throw an exception:
        _exception = t
        proc.destroy()
        logError("Thread-ScriptTransformation-Feed exit cause by: ", t)
    } finally {
      try {
        Utils.tryLogNonFatalError(outputStream.close())
        if (proc.waitFor() != 0) {
          logError(stderrBuffer.toString) // log the stderr circular buffer
        }
      } catch {
        case NonFatal(exceptionFromFinallyBlock) =>
          if (!threwException) {
            throw exceptionFromFinallyBlock
          } else {
            log.error("Exception in finally block", exceptionFromFinallyBlock)
          }
      }
    }
  }
}

/**
 * The wrapper class of Hive input and output schema properties
 */
class ScriptTransformIOSchema (
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: Option[String],
    outputSerdeClass: Option[String],
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReaderClass: Option[String],
    recordWriterClass: Option[String],
    schemaLess: Boolean) extends Serializable {

  protected val defaultFormat = Map(
    ("TOK_TABLEROWFORMATFIELD", "\t"),
    ("TOK_TABLEROWFORMATLINES", "\n")
  )

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))

  def isSchemaLess: Boolean = schemaLess
}
