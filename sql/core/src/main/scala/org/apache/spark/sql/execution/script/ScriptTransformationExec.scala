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

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{SQLDate, SQLTimestamp}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, DateType, StructType, TimestampType}
import org.apache.spark.util.{CircularBuffer, RedirectThread, SerializableConfiguration, Utils}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
private[sql]
case class ScriptTransformationExec(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: ScriptTransformIOSchema)
  extends UnaryExecNode with ScriptTransformBase {

  override def producedAttributes: AttributeSet = outputSet -- inputSet

  protected override def doExecute(): RDD[InternalRow] =
    execute(sqlContext, child, schema)

  override def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration) : Iterator[InternalRow] = {

    val (proc, inputStream, outputStream, stderrBuffer, outputProjection) =
      init(input, script, child)

    // This new thread will consume the ScriptTransformation's input rows and write them to the
    // external process. That process's output will be read by this current thread.
    val writerThread = new ScriptTransformationWriterThread(
      inputIterator,
      input.map(_.dataType),
      outputProjection,
      ioschema,
      outputStream,
      proc,
      stderrBuffer,
      TaskContext.get(),
      hadoopConf
    )

    val reader = createReader(inputStream)

    val outputIterator: Iterator[InternalRow] = new Iterator[InternalRow] {
      var curLine: String = null
      val mutableRow = new SpecificInternalRow(output.map(_.dataType))
      val fieldDelimiter = ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD")

      override def hasNext: Boolean = {
        try {
          if (curLine == null) {
            curLine = reader.readLine()
            if (curLine == null) {
              checkFailureAndPropagate(writerThread.exception, null, proc, stderrBuffer)
              return false
            }
          }
          true
        } catch {
          case NonFatal(e) =>
            // If this exception is due to abrupt / unclean termination of `proc`,
            // then detect it and propagate a better exception message for end users
            checkFailureAndPropagate(writerThread.exception, e, proc, stderrBuffer)

            throw e
        }
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val prevLine = curLine
        curLine = reader.readLine()
        if (!ioschema.isSchemaLess) {
          new GenericInternalRow(
            prevLine.split(fieldDelimiter).map(CatalystTypeConverters.convertToCatalyst))
        } else {
          new GenericInternalRow(
            prevLine.split(fieldDelimiter, 2).map(CatalystTypeConverters.convertToCatalyst))
        }
      }
    }

    writerThread.start()
    outputIterator
  }
}

private[sql] trait ScriptTransformBase extends Serializable with Logging {

  def init(
      input: Seq[Expression],
      script: String,
      child: SparkPlan
    ): (Process, InputStream, OutputStream, CircularBuffer, InterpretedProjection) = {

    val cmd = List("/bin/bash", "-c", script)
    val builder = new ProcessBuilder(cmd.asJava)

    val proc = builder.start()
    val inputStream = proc.getInputStream
    val outputStream = proc.getOutputStream
    val errorStream = proc.getErrorStream

    // In order to avoid deadlocks, we need to consume the error output of the child process.
    // To avoid issues caused by large error output, we use a circular buffer to limit the amount
    // of error output that we retain. See SPARK-7862 for more discussion of the deadlock / hang
    // that motivates this.
    val stderrBuffer = new CircularBuffer(2048)
    new RedirectThread(
      errorStream,
      stderrBuffer,
      "Thread-ScriptTransformation-STDERR-Consumer").start()

    val outputProjection = new InterpretedProjection(input, child.output)
    (proc, inputStream, outputStream, stderrBuffer, outputProjection)
  }

  def execute(sqlContext: SQLContext,
              child: SparkPlan,
              schema: StructType): RDD[InternalRow] = {
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

  def checkFailureAndPropagate(
      writerException: Option[Throwable],
      cause: Throwable = null,
      proc: Process,
      stderrBuffer: CircularBuffer): Unit = {
    if (writerException.isDefined) {
      throw writerException.get
    }

    // Checks if the proc is still alive (incase the command ran was bad)
    // The ideal way to do this is to use Java 8's Process#isAlive()
    // but it cannot be used because Spark still supports Java 7.
    // Following is a workaround used to check if a process is alive in Java 7
    // TODO: Once builds are switched to Java 8, this can be changed
    try {
      val exitCode = proc.exitValue()
      if (exitCode != 0) {
        logError(stderrBuffer.toString) // log the stderr circular buffer
        throw new SparkException(s"Subprocess exited with status $exitCode. " +
          s"Error: ${stderrBuffer.toString}", cause)
      }
    } catch {
      case _: IllegalThreadStateException =>
      // This means that the process is still alive. Move ahead
    }
  }

  def createReader(inputStream: InputStream): BufferedReader =
    new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))

  def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration) : Iterator[InternalRow]
}

private[sql] class ScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    outputProjection: Projection,
    ioschema: ScriptTransformIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration
  ) extends Thread("Thread-ScriptTransformation-Feed") with Logging with Serializable {

  setDaemon(true)

  @volatile protected var _exception: Throwable = null

  protected val lineDelimiter = ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES")
  protected val fieldDelimiter = ioschema.inputRowFormatMap("TOK_TABLEROWFORMATFIELD")

  /** Contains the exception thrown while writing the parent iterator to the external process. */
  def exception: Option[Throwable] = Option(_exception)

  protected def init(): Unit = {
    TaskContext.setTaskContext(taskContext)
  }

  protected def processRow(row: InternalRow, numColumns: Int): Unit = {
    val data = if (numColumns == 0) {
      lineDelimiter
    } else {
      val sb = new StringBuilder
      sb.append(row.get(0, inputSchema.head))
      var i = 1
      while (i < numColumns) {
        sb.append(fieldDelimiter)
        val columnType = inputSchema(i)
        val fieldValue = row.get(i, columnType)
        val fieldStringValue = columnType match {
          case _: DateType =>
            DateTimeUtils.dateToString(fieldValue.asInstanceOf[SQLDate])
          case _: TimestampType =>
            DateTimeUtils.timestampToString(fieldValue.asInstanceOf[SQLTimestamp])
          case _ =>
            fieldValue.toString
        }
        sb.append(fieldStringValue)
        i += 1
      }
      sb.append(lineDelimiter)
      sb.toString()
    }
    outputStream.write(data.getBytes(StandardCharsets.UTF_8))
  }

  override def run(): Unit = Utils.logUncaughtExceptions {
    init()

    // We can't use Utils.tryWithSafeFinally here because we also need a `catch` block, so
    // let's use a variable to record whether the `finally` block was hit due to an exception
    var threwException: Boolean = true
    val numColumns = inputSchema.length
    try {
      iter.map(outputProjection).foreach(row => processRow(row, numColumns))
      threwException = false
    } catch {
      case t: Throwable =>
        // An error occurred while writing input, so kill the child process. According to the
        // Javadoc this call will not throw an exception:
        _exception = t
        proc.destroy()
        throw t
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

private[sql]
object ScriptTransformIOSchema {
  def apply(input: ScriptInputOutputSchema): ScriptTransformIOSchema = {
    new ScriptTransformIOSchema(
      input.inputRowFormat,
      input.outputRowFormat,
      input.schemaLess)
  }
}

/**
 * The wrapper class of Hive input and output schema properties
 *
 * @param inputRowFormat Contains delimiter information for the script's output
 * @param outputRowFormat Contains delimiter information for the script's input
 * @param schemaLess When set to true, script's output is tokenized as a key-value pair
 *                   else it would be tokenized to extract multiple columns.
 */
private[sql] class ScriptTransformIOSchema (
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    schemaLess: Boolean) extends Serializable {

  private val defaultFormat = Map(
    ("TOK_TABLEROWFORMATFIELD", "\t"),
    ("TOK_TABLEROWFORMATLINES", "\n")
  )

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))

  def isSchemaLess: Boolean = schemaLess
}
