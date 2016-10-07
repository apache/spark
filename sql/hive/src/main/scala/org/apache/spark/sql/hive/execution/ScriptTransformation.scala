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

package org.apache.spark.sql.hive.execution

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Properties
import javax.annotation.Nullable

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{RecordReader, RecordWriter}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.io.Writable

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveInspectors
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{CircularBuffer, RedirectThread, SerializableConfiguration, Utils}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: HiveScriptIOSchema)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = outputSet -- inputSet

  protected override def doExecute(): RDD[InternalRow] = {
    def processIterator(inputIterator: Iterator[InternalRow], hadoopConf: Configuration)
      : Iterator[InternalRow] = {
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

      // This nullability is a performance optimization in order to avoid an Option.foreach() call
      // inside of a loop
      @Nullable val (inputSerde, inputSoi) = ioschema.initInputSerDe(input).getOrElse((null, null))

      // This new thread will consume the ScriptTransformation's input rows and write them to the
      // external process. That process's output will be read by this current thread.
      val writerThread = new ScriptTransformationWriterThread(
        inputIterator,
        input.map(_.dataType),
        outputProjection,
        inputSerde,
        inputSoi,
        ioschema,
        outputStream,
        proc,
        stderrBuffer,
        TaskContext.get(),
        hadoopConf
      )

      // This nullability is a performance optimization in order to avoid an Option.foreach() call
      // inside of a loop
      @Nullable val (outputSerde, outputSoi) = {
        ioschema.initOutputSerDe(output).getOrElse((null, null))
      }

      val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
      val outputIterator: Iterator[InternalRow] = new Iterator[InternalRow] with HiveInspectors {
        var curLine: String = null
        val scriptOutputStream = new DataInputStream(inputStream)

        @Nullable val scriptOutputReader =
          ioschema.recordReader(scriptOutputStream, hadoopConf).orNull

        var scriptOutputWritable: Writable = null
        val reusedWritableObject: Writable = if (null != outputSerde) {
          outputSerde.getSerializedClass().newInstance
        } else {
          null
        }
        val mutableRow = new SpecificInternalRow(output.map(_.dataType))

        @transient
        lazy val unwrappers = outputSoi.getAllStructFieldRefs.asScala.map(unwrapperFor)

        private def checkFailureAndPropagate(cause: Throwable = null): Unit = {
          if (writerThread.exception.isDefined) {
            throw writerThread.exception.get
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

        override def hasNext: Boolean = {
          try {
            if (outputSerde == null) {
              if (curLine == null) {
                curLine = reader.readLine()
                if (curLine == null) {
                  checkFailureAndPropagate()
                  return false
                }
              }
            } else if (scriptOutputWritable == null) {
              scriptOutputWritable = reusedWritableObject

              if (scriptOutputReader != null) {
                if (scriptOutputReader.next(scriptOutputWritable) <= 0) {
                  checkFailureAndPropagate()
                  return false
                }
              } else {
                try {
                  scriptOutputWritable.readFields(scriptOutputStream)
                } catch {
                  case _: EOFException =>
                    // This means that the stdout of `proc` (ie. TRANSFORM process) has exhausted.
                    // Ideally the proc should *not* be alive at this point but
                    // there can be a lag between EOF being written out and the process
                    // being terminated. So explicitly waiting for the process to be done.
                    proc.waitFor()
                    checkFailureAndPropagate()
                    return false
                }
              }
            }

            true
          } catch {
            case NonFatal(e) =>
              // If this exception is due to abrupt / unclean termination of `proc`,
              // then detect it and propagate a better exception message for end users
              checkFailureAndPropagate(e)

              throw e
          }
        }

        override def next(): InternalRow = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          if (outputSerde == null) {
            val prevLine = curLine
            curLine = reader.readLine()
            if (!ioschema.schemaLess) {
              new GenericInternalRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
                  .map(CatalystTypeConverters.convertToCatalyst))
            } else {
              new GenericInternalRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"), 2)
                  .map(CatalystTypeConverters.convertToCatalyst))
            }
          } else {
            val raw = outputSerde.deserialize(scriptOutputWritable)
            scriptOutputWritable = null
            val dataList = outputSoi.getStructFieldsDataAsList(raw)
            var i = 0
            while (i < dataList.size()) {
              if (dataList.get(i) == null) {
                mutableRow.setNullAt(i)
              } else {
                unwrappers(i)(dataList.get(i), mutableRow, i)
              }
              i += 1
            }
            mutableRow
          }
        }
      }

      writerThread.start()

      outputIterator
    }

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
}

private class ScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    outputProjection: Projection,
    @Nullable inputSerde: AbstractSerDe,
    @Nullable inputSoi: ObjectInspector,
    ioschema: HiveScriptIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration
  ) extends Thread("Thread-ScriptTransformation-Feed") with Logging {

  setDaemon(true)

  @volatile private var _exception: Throwable = null

  /** Contains the exception thrown while writing the parent iterator to the external process. */
  def exception: Option[Throwable] = Option(_exception)

  override def run(): Unit = Utils.logUncaughtExceptions {
    TaskContext.setTaskContext(taskContext)

    val dataOutputStream = new DataOutputStream(outputStream)
    @Nullable val scriptInputWriter = ioschema.recordWriter(dataOutputStream, conf).orNull

    // We can't use Utils.tryWithSafeFinally here because we also need a `catch` block, so
    // let's use a variable to record whether the `finally` block was hit due to an exception
    var threwException: Boolean = true
    val len = inputSchema.length
    try {
      iter.map(outputProjection).foreach { row =>
        if (inputSerde == null) {
          val data = if (len == 0) {
            ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES")
          } else {
            val sb = new StringBuilder
            sb.append(row.get(0, inputSchema(0)))
            var i = 1
            while (i < len) {
              sb.append(ioschema.inputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
              sb.append(row.get(i, inputSchema(i)))
              i += 1
            }
            sb.append(ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES"))
            sb.toString()
          }
          outputStream.write(data.getBytes(StandardCharsets.UTF_8))
        } else {
          val writable = inputSerde.serialize(
            row.asInstanceOf[GenericInternalRow].values, inputSoi)

          if (scriptInputWriter != null) {
            scriptInputWriter.write(writable)
          } else {
            prepareWritable(writable, ioschema.outputSerdeProps).write(dataOutputStream)
          }
        }
      }
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

object HiveScriptIOSchema {
  def apply(input: ScriptInputOutputSchema): HiveScriptIOSchema = {
    HiveScriptIOSchema(
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

/**
 * The wrapper class of Hive input and output schema properties
 */
case class HiveScriptIOSchema (
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: Option[String],
    outputSerdeClass: Option[String],
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReaderClass: Option[String],
    recordWriterClass: Option[String],
    schemaLess: Boolean)
  extends HiveInspectors {

  private val defaultFormat = Map(
    ("TOK_TABLEROWFORMATFIELD", "\t"),
    ("TOK_TABLEROWFORMATLINES", "\n")
  )

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))


  def initInputSerDe(input: Seq[Expression]): Option[(AbstractSerDe, ObjectInspector)] = {
    inputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(input)
      val serde = initSerDe(serdeClass, columns, columnTypes, inputSerdeProps)
      val fieldObjectInspectors = columnTypes.map(toInspector)
      val objectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columns.asJava, fieldObjectInspectors.asJava)
        .asInstanceOf[ObjectInspector]
      (serde, objectInspector)
    }
  }

  def initOutputSerDe(output: Seq[Attribute]): Option[(AbstractSerDe, StructObjectInspector)] = {
    outputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(output)
      val serde = initSerDe(serdeClass, columns, columnTypes, outputSerdeProps)
      val structObjectInspector = serde.getObjectInspector().asInstanceOf[StructObjectInspector]
      (serde, structObjectInspector)
    }
  }

  private def parseAttrs(attrs: Seq[Expression]): (Seq[String], Seq[DataType]) = {
    val columns = attrs.zipWithIndex.map(e => s"${e._1.prettyName}_${e._2}")
    val columnTypes = attrs.map(_.dataType)
    (columns, columnTypes)
  }

  private def initSerDe(
      serdeClassName: String,
      columns: Seq[String],
      columnTypes: Seq[DataType],
      serdeProps: Seq[(String, String)]): AbstractSerDe = {

    val serde = Utils.classForName(serdeClassName).newInstance.asInstanceOf[AbstractSerDe]

    val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")

    var propsMap = serdeProps.toMap + (serdeConstants.LIST_COLUMNS -> columns.mkString(","))
    propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypesNames)

    val properties = new Properties()
    properties.putAll(propsMap.asJava)
    serde.initialize(null, properties)

    serde
  }

  def recordReader(
      inputStream: InputStream,
      conf: Configuration): Option[RecordReader] = {
    recordReaderClass.map { klass =>
      val instance = Utils.classForName(klass).newInstance().asInstanceOf[RecordReader]
      val props = new Properties()
      props.putAll(outputSerdeProps.toMap.asJava)
      instance.initialize(inputStream, conf, props)
      instance
    }
  }

  def recordWriter(outputStream: OutputStream, conf: Configuration): Option[RecordWriter] = {
    recordWriterClass.map { klass =>
      val instance = Utils.classForName(klass).newInstance().asInstanceOf[RecordWriter]
      instance.initialize(outputStream, conf)
      instance
    }
  }
}
