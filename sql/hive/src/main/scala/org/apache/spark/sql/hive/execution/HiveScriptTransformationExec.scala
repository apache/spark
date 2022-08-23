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
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{RecordReader, RecordWriter}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.io.Writable

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveInspectors
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{CircularBuffer, Utils}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 * @param child logical plan whose output is transformed.
 * @param ioschema the class set that defines how to handle input/output data.
 */
private[hive] case class HiveScriptTransformationExec(
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: ScriptTransformationIOSchema)
  extends BaseScriptTransformationExec {
  import HiveScriptIOSchema._

  private def createOutputIteratorWithSerde(
      writerThread: BaseScriptTransformationWriterThread,
      inputStream: InputStream,
      proc: Process,
      stderrBuffer: CircularBuffer,
      outputSerde: AbstractSerDe,
      outputSoi: StructObjectInspector,
      hadoopConf: Configuration): Iterator[InternalRow] = {
    new Iterator[InternalRow] with HiveInspectors {
      private var completed = false
      val scriptOutputStream = new DataInputStream(inputStream)

      val scriptOutputReader =
        recordReader(ioschema, scriptOutputStream, hadoopConf).orNull

      var scriptOutputWritable: Writable = null
      val reusedWritableObject = outputSerde.getSerializedClass.getConstructor().newInstance()
      val mutableRow = new SpecificInternalRow(output.map(_.dataType))

      @transient
      lazy val unwrappers = outputSoi.getAllStructFieldRefs.asScala.map(unwrapperFor)

      override def hasNext: Boolean = {
        if (completed) {
          return false
        }
        try {
          if (scriptOutputWritable == null) {
            scriptOutputWritable = reusedWritableObject

            if (scriptOutputReader != null) {
              if (scriptOutputReader.next(scriptOutputWritable) <= 0) {
                checkFailureAndPropagate(writerThread, null, proc, stderrBuffer)
                completed = true
                return false
              }
            } else {
              try {
                scriptOutputWritable.readFields(scriptOutputStream)
              } catch {
                case _: EOFException =>
                  // This means that the stdout of `proc` (i.e. TRANSFORM process) has exhausted.
                  // Ideally the proc should *not* be alive at this point but
                  // there can be a lag between EOF being written out and the process
                  // being terminated. So explicitly waiting for the process to be done.
                  checkFailureAndPropagate(writerThread, null, proc, stderrBuffer)
                  completed = true
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

  override def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration): Iterator[InternalRow] = {

    val (outputStream, proc, inputStream, stderrBuffer) = initProc

    val (inputSerde, inputSoi) = initInputSerDe(ioschema, child.output).getOrElse((null, null))

    // For HiveScriptTransformationExec, if inputSerde == null, but outputSerde != null
    // We will use StringBuffer to pass data, in this case, we should cast data as string too.
    val finalInput = if (inputSerde == null) {
      inputExpressionsWithoutSerde
    } else {
      child.output
    }

    val outputProjection = new InterpretedProjection(finalInput, child.output)

    // This new thread will consume the ScriptTransformation's input rows and write them to the
    // external process. That process's output will be read by this current thread.
    val writerThread = HiveScriptTransformationWriterThread(
      inputIterator.map(outputProjection),
      finalInput.map(_.dataType),
      inputSerde,
      inputSoi,
      ioschema,
      outputStream,
      proc,
      stderrBuffer,
      TaskContext.get(),
      hadoopConf
    )

    val (outputSerde, outputSoi) = {
      initOutputSerDe(ioschema, output).getOrElse((null, null))
    }

    val outputIterator = if (outputSerde == null) {
      createOutputIteratorWithoutSerde(writerThread, inputStream, proc, stderrBuffer)
    } else {
      createOutputIteratorWithSerde(
        writerThread, inputStream, proc, stderrBuffer, outputSerde, outputSoi, hadoopConf)
    }

    writerThread.start()

    outputIterator
  }

  override protected def withNewChildInternal(newChild: SparkPlan): HiveScriptTransformationExec =
    copy(child = newChild)
}

private[hive] case class HiveScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    inputSerde: AbstractSerDe,
    inputSoi: StructObjectInspector,
    ioSchema: ScriptTransformationIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration)
  extends BaseScriptTransformationWriterThread with HiveInspectors {
  import HiveScriptIOSchema._

  override def processRows(): Unit = {
    val dataOutputStream = new DataOutputStream(outputStream)
    val scriptInputWriter = recordWriter(ioSchema, dataOutputStream, conf).orNull

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

object HiveScriptIOSchema extends HiveInspectors {

  def initInputSerDe(
      ioschema: ScriptTransformationIOSchema,
      input: Seq[Expression]): Option[(AbstractSerDe, StructObjectInspector)] = {
    ioschema.inputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(input)
      val serde = initSerDe(serdeClass, columns, columnTypes, ioschema.inputSerdeProps)
      val fieldObjectInspectors = columnTypes.map(toInspector)
      val objectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columns.asJava, fieldObjectInspectors.asJava)
      (serde, objectInspector)
    }
  }

  def initOutputSerDe(
      ioschema: ScriptTransformationIOSchema,
      output: Seq[Attribute]): Option[(AbstractSerDe, StructObjectInspector)] = {
    ioschema.outputSerdeClass.map { serdeClass =>
      val (columns, columnTypes) = parseAttrs(output)
      val serde = initSerDe(serdeClass, columns, columnTypes, ioschema.outputSerdeProps)
      val structObjectInspector = serde.getObjectInspector().asInstanceOf[StructObjectInspector]
      (serde, structObjectInspector)
    }
  }

  private def parseAttrs(attrs: Seq[Expression]): (Seq[String], Seq[DataType]) = {
    val columns = attrs.zipWithIndex.map(e => s"${e._1.prettyName}_${e._2}")
    val columnTypes = attrs.map(_.dataType)
    (columns, columnTypes)
  }

  def initSerDe(
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
      ioschema: ScriptTransformationIOSchema,
      inputStream: InputStream,
      conf: Configuration): Option[RecordReader] = {
    ioschema.recordReaderClass.map { klass =>
      val instance = Utils.classForName[RecordReader](klass).getConstructor().
        newInstance()
      val props = new Properties()
      // Can not use props.putAll(outputSerdeProps.toMap.asJava) in scala-2.12
      // See https://github.com/scala/bug/issues/10418
      ioschema.outputSerdeProps.toMap.foreach { case (k, v) => props.put(k, v) }
      instance.initialize(inputStream, conf, props)
      instance
    }
  }

  def recordWriter(
      ioschema: ScriptTransformationIOSchema,
      outputStream: OutputStream,
      conf: Configuration): Option[RecordWriter] = {
    ioschema.recordWriterClass.map { klass =>
      val instance = Utils.classForName[RecordWriter](klass).getConstructor().
        newInstance()
      instance.initialize(outputStream, conf)
      instance
    }
  }
}
