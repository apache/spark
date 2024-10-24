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

package org.apache.spark.sql.execution

import java.io.{BufferedReader, File, InputStream, InputStreamReader, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkFiles, TaskContext}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Cast, Expression, GenericInternalRow, JsonToStructs, Literal, StructsToJson, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CircularBuffer, RedirectThread, SerializableConfiguration, Utils}

trait BaseScriptTransformationExec extends UnaryExecNode {
  def script: String
  def output: Seq[Attribute]
  def child: SparkPlan
  def ioschema: ScriptTransformationIOSchema

  protected lazy val inputExpressionsWithoutSerde: Seq[Expression] = {
    child.output.map { in =>
      in.dataType match {
        case _: ArrayType | _: MapType | _: StructType =>
          StructsToJson(ioschema.inputSerdeProps.toMap, in,
            Some(conf.sessionLocalTimeZone)).replacement
        case _ => Cast(in, StringType).withTimeZone(conf.sessionLocalTimeZone)
      }
    }
  }

  override def producedAttributes: AttributeSet = outputSet -- inputSet

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] = {
    val broadcastedHadoopConf =
      new SerializableConfiguration(session.sessionState.newHadoopConf())

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

  protected def initProc: (OutputStream, Process, InputStream, CircularBuffer) = {
    val cmd = List("/bin/bash", "-c", script)
    val builder = new ProcessBuilder(cmd.asJava)
      .directory(new File(SparkFiles.getRootDirectory()))
    val path = System.getenv("PATH") + File.pathSeparator +
      SparkFiles.getRootDirectory()
    builder.environment().put("PATH", path)

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
      s"Thread-${this.getClass.getSimpleName}-STDERR-Consumer").start()
    (outputStream, proc, inputStream, stderrBuffer)
  }

  protected def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration): Iterator[InternalRow]

  protected def createOutputIteratorWithoutSerde(
      writerThread: BaseScriptTransformationWriterThread,
      inputStream: InputStream,
      proc: Process,
      stderrBuffer: CircularBuffer): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      var curLine: String = null
      val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))

      val outputRowFormat = ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD")
      val processRowWithoutSerde = if (!ioschema.schemaLess) {
        prevLine: String =>
          new GenericInternalRow(
            prevLine.split(outputRowFormat, -1).padTo(outputFieldWriters.size, null)
              .zip(outputFieldWriters)
              .map { case (data, writer) => writer(data) })
      } else {
        // In schema less mode, hive will choose first two output column as output.
        // If output column size less than 2, it will return NULL for columns with missing values.
        // Here we split row string and choose first 2 values, if values's size less than 2,
        // we pad NULL value until 2 to make behavior same with hive.
        val kvWriter = CatalystTypeConverters.createToCatalystConverter(StringType)
        prevLine: String =>
          new GenericInternalRow(
            prevLine.split(outputRowFormat, -1).slice(0, 2).padTo(2, null)
              .map(kvWriter))
      }

      override def hasNext: Boolean = {
        try {
          if (curLine == null) {
            curLine = reader.readLine()
            if (curLine == null) {
              checkFailureAndPropagate(writerThread, null, proc, stderrBuffer)
              return false
            }
          }
          true
        } catch {
          case NonFatal(e) =>
            // If this exception is due to abrupt / unclean termination of `proc`,
            // then detect it and propagate a better exception message for end users
            checkFailureAndPropagate(writerThread, e, proc, stderrBuffer)

            throw e
        }
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val prevLine = curLine
        curLine = reader.readLine()
        processRowWithoutSerde(prevLine)
      }
    }
  }

  protected def checkFailureAndPropagate(
      writerThread: BaseScriptTransformationWriterThread,
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
        logError(log"${MDC(STDERR, stderrBuffer.toString)}") // log the stderr circular buffer
        throw QueryExecutionErrors.subprocessExitedError(exitCode, stderrBuffer, cause)
      }
    }
  }

  private lazy val outputFieldWriters: Seq[String => Any] = output.map { attr =>
    val converter = CatalystTypeConverters.createToCatalystConverter(attr.dataType)
    attr.dataType match {
      case StringType => wrapperConvertException(data => data, converter)
      case BooleanType => wrapperConvertException(data => data.toBoolean, converter)
      case ByteType => wrapperConvertException(data => data.toByte, converter)
      case BinaryType =>
        wrapperConvertException(data => UTF8String.fromString(data).getBytes, converter)
      case IntegerType => wrapperConvertException(data => data.toInt, converter)
      case ShortType => wrapperConvertException(data => data.toShort, converter)
      case LongType => wrapperConvertException(data => data.toLong, converter)
      case FloatType => wrapperConvertException(data => data.toFloat, converter)
      case DoubleType => wrapperConvertException(data => data.toDouble, converter)
      case _: DecimalType => wrapperConvertException(data => BigDecimal(data), converter)
      case DateType if conf.datetimeJava8ApiEnabled =>
        wrapperConvertException(data => DateTimeUtils.stringToDate(UTF8String.fromString(data))
          .map(DateTimeUtils.daysToLocalDate).orNull, converter)
      case DateType =>
        wrapperConvertException(data => DateTimeUtils.stringToDate(UTF8String.fromString(data))
          .map(DateTimeUtils.toJavaDate).orNull, converter)
      case TimestampType if conf.datetimeJava8ApiEnabled =>
        wrapperConvertException(data => DateTimeUtils.stringToTimestamp(
          UTF8String.fromString(data),
          DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
          .map(DateTimeUtils.microsToInstant).orNull, converter)
      case TimestampType => wrapperConvertException(data => DateTimeUtils.stringToTimestamp(
        UTF8String.fromString(data),
        DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
        .map(DateTimeUtils.toJavaTimestamp).orNull, converter)
      case TimestampNTZType =>
        wrapperConvertException(data => DateTimeUtils.stringToTimestampWithoutTimeZone(
          UTF8String.fromString(data)).map(DateTimeUtils.microsToLocalDateTime).orNull, converter)
      case CalendarIntervalType => wrapperConvertException(
        data => IntervalUtils.stringToInterval(UTF8String.fromString(data)),
        converter)
      case YearMonthIntervalType(start, end) => wrapperConvertException(
        data => IntervalUtils.monthsToPeriod(
          IntervalUtils.castStringToYMInterval(UTF8String.fromString(data), start, end)),
        converter)
      case DayTimeIntervalType(start, end) => wrapperConvertException(
        data => IntervalUtils.microsToDuration(
          IntervalUtils.castStringToDTInterval(UTF8String.fromString(data), start, end)),
        converter)
      case _: ArrayType | _: MapType | _: StructType =>
        val complexTypeFactory = JsonToStructs(attr.dataType,
          ioschema.outputSerdeProps.toMap, Literal(null), Some(conf.sessionLocalTimeZone))
        wrapperConvertException(data =>
          complexTypeFactory.evaluator.evaluate(UTF8String.fromString(data)), any => any)
      case udt: UserDefinedType[_] =>
        wrapperConvertException(data => udt.deserialize(data), converter)
      case dt =>
        throw QueryExecutionErrors.outputDataTypeUnsupportedByNodeWithoutSerdeError(nodeName, dt)
    }
  }

  // Keep consistent with Hive `LazySimpleSerde`, when there is a type case error, return null
  private val wrapperConvertException: (String => Any, Any => Any) => String => Any =
    (f: String => Any, converter: Any => Any) =>
      (data: String) => converter {
        if (data == ioschema.outputRowFormatMap("TOK_TABLEROWFORMATNULL")) {
          null
        } else {
          try {
            f(data)
          } catch {
            case NonFatal(_) => null
          }
        }
      }
}

abstract class BaseScriptTransformationWriterThread extends Thread with Logging {

  def iter: Iterator[InternalRow]
  def inputSchema: Seq[DataType]
  def ioSchema: ScriptTransformationIOSchema
  def outputStream: OutputStream
  def proc: Process
  def stderrBuffer: CircularBuffer
  def taskContext: TaskContext
  def conf: Configuration

  setName(s"Thread-${this.getClass.getSimpleName}-Feed")
  setDaemon(true)

  @volatile protected var _exception: Throwable = null

  /** Contains the exception thrown while writing the parent iterator to the external process. */
  def exception: Option[Throwable] = Option(_exception)

  protected def processRows(): Unit

  protected def processRowsWithoutSerde(): Unit = {
    val len = inputSchema.length
    iter.foreach { row =>
      val data = if (len == 0) {
        ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATLINES")
      } else {
        val sb = new StringBuilder
        def appendToBuffer(s: AnyRef): Unit = {
          if (s == null) {
            sb.append(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATNULL"))
          } else {
            sb.append(s)
          }
        }
        appendToBuffer(row.get(0, inputSchema(0)))
        var i = 1
        while (i < len) {
          sb.append(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
          appendToBuffer(row.get(i, inputSchema(i)))
          i += 1
        }
        sb.append(ioSchema.inputRowFormatMap("TOK_TABLEROWFORMATLINES"))
        sb.toString()
      }
      outputStream.write(data.getBytes(StandardCharsets.UTF_8))
    }
  }

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
        logError(log"Thread-${MDC(CLASS_NAME, this.getClass.getSimpleName)}-Feed " +
          log"exit cause by: ", t)
    } finally {
      try {
        Utils.tryLogNonFatalError(outputStream.close())
        if (proc.waitFor() != 0) {
          logError(log"${MDC(STDERR, stderrBuffer.toString)}") // log the stderr circular buffer
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
 * The wrapper class of input and output schema properties
 */
case class ScriptTransformationIOSchema(
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: Option[String],
    outputSerdeClass: Option[String],
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReaderClass: Option[String],
    recordWriterClass: Option[String],
    schemaLess: Boolean) extends Serializable {
  import ScriptTransformationIOSchema._

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))
}

object ScriptTransformationIOSchema {
  val defaultFormat = Map(
    ("TOK_TABLEROWFORMATFIELD", "\u0001"),
    ("TOK_TABLEROWFORMATLINES", "\n"),
    ("TOK_TABLEROWFORMATNULL" -> "\\N")
  )

  val defaultIOSchema = ScriptTransformationIOSchema(
    inputRowFormat = Seq.empty,
    outputRowFormat = Seq.empty,
    inputSerdeClass = None,
    outputSerdeClass = None,
    inputSerdeProps = Seq.empty,
    outputSerdeProps = Seq.empty,
    recordReaderClass = None,
    recordWriterClass = None,
    schemaLess = false
  )

  def apply(input: ScriptInputOutputSchema): ScriptTransformationIOSchema = {
    ScriptTransformationIOSchema(
      input.inputRowFormat,
      input.outputRowFormat,
      input.inputSerdeClass,
      input.outputSerdeClass,
      input.inputSerdeProps,
      input.outputSerdeProps,
      input.recordReaderClass,
      input.recordWriterClass,
      input.schemaLess)
  }
}
