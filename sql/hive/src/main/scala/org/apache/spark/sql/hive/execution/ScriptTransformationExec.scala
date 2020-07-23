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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveInspectors
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{CircularBuffer, RedirectThread, Utils}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class HiveScriptTransformationExec(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: HiveScriptIOSchema)
  extends BaseScriptTransformationExec {

  override def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration): Iterator[InternalRow] = {
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
    val writerThread = new HiveScriptTransformationWriterThread(
      inputIterator.map(outputProjection),
      input.map(_.dataType),
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
        outputSerde.getSerializedClass().getConstructor().newInstance()
      } else {
        null
      }
      val mutableRow = new SpecificInternalRow(output.map(_.dataType))

      @transient
      lazy val unwrappers = outputSoi.getAllStructFieldRefs.asScala.map(unwrapperFor)

      override def hasNext: Boolean = {
        try {
          if (outputSerde == null) {
            if (curLine == null) {
              curLine = reader.readLine()
              if (curLine == null) {
                checkFailureAndPropagate(writerThread, null, proc, stderrBuffer)
                return false
              }
            }
          } else if (scriptOutputWritable == null) {
            scriptOutputWritable = reusedWritableObject

            if (scriptOutputReader != null) {
              if (scriptOutputReader.next(scriptOutputWritable) <= 0) {
                checkFailureAndPropagate(writerThread, null, proc, stderrBuffer)
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
                  checkFailureAndPropagate(writerThread, null, proc, stderrBuffer)
                  return false
              }
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
}

private class HiveScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    @Nullable inputSerde: AbstractSerDe,
    @Nullable inputSoi: StructObjectInspector,
    ioSchema: HiveScriptIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration)
  extends BaseScriptTransformationWriterThread(
    iter,
    inputSchema,
    ioSchema,
    outputStream,
    proc,
    stderrBuffer,
    taskContext,
    conf) with HiveInspectors {

  override def processRows(): Unit = {
    val dataOutputStream = new DataOutputStream(outputStream)
    @Nullable val scriptInputWriter = ioSchema.recordWriter(dataOutputStream, conf).orNull

    if (inputSerde == null) {
      processRowsWithoutSerde()
    } else {
      // Convert Spark InternalRows to hive data via `HiveInspectors.wrapperFor`.
      val hiveData = new Array[Any](inputSchema.length)
      val fieldOIs = inputSoi.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
      val wrappers = fieldOIs.zip(inputSchema).map { case (f, dt) => wrapperFor(f, dt) }

      iter.foreach { row =>
        var i = 0
        while (i < fieldOIs.length) {
          hiveData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, inputSchema(i)))
          i += 1
        }

        val writable = inputSerde.serialize(hiveData, inputSoi)
        if (scriptInputWriter != null) {
          scriptInputWriter.write(writable)
        } else {
          prepareWritable(writable, ioSchema.outputSerdeProps).write(dataOutputStream)
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
  extends BaseScriptTransformIOSchema with HiveInspectors {

  def initInputSerDe(input: Seq[Expression]): Option[(AbstractSerDe, StructObjectInspector)] = {
    inputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(input)
      val serde = initSerDe(serdeClass, columns, columnTypes, inputSerdeProps)
      val fieldObjectInspectors = columnTypes.map(toInspector)
      val objectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columns.asJava, fieldObjectInspectors.asJava)
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

    val serde = Utils.classForName[AbstractSerDe](serdeClassName).getConstructor().
      newInstance()

    val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")

    var propsMap = serdeProps.toMap + (serdeConstants.LIST_COLUMNS -> columns.mkString(","))
    propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypesNames)

    val properties = new Properties()
    // Can not use properties.putAll(propsMap.asJava) in scala-2.12
    // See https://github.com/scala/bug/issues/10418
    propsMap.foreach { case (k, v) => properties.put(k, v) }
    serde.initialize(null, properties)

    serde
  }

  def recordReader(
      inputStream: InputStream,
      conf: Configuration): Option[RecordReader] = {
    recordReaderClass.map { klass =>
      val instance = Utils.classForName[RecordReader](klass).getConstructor().
        newInstance()
      val props = new Properties()
      // Can not use props.putAll(outputSerdeProps.toMap.asJava) in scala-2.12
      // See https://github.com/scala/bug/issues/10418
      outputSerdeProps.toMap.foreach { case (k, v) => props.put(k, v) }
      instance.initialize(inputStream, conf, props)
      instance
    }
  }

  def recordWriter(outputStream: OutputStream, conf: Configuration): Option[RecordWriter] = {
    recordWriterClass.map { klass =>
      val instance = Utils.classForName[RecordWriter](klass).getConstructor().
        newInstance()
      instance.initialize(outputStream, conf)
      instance
    }
  }
}
