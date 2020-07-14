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

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.types._
import org.apache.spark.util.{CircularBuffer, RedirectThread}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class SparkScriptTransformationExec(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: SparkScriptIOSchema)
  extends BaseScriptTransformationExec {

  override def processIterator(inputIterator: Iterator[InternalRow], hadoopConf: Configuration)
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

    val finalInput = input.map(Cast(_, StringType).withTimeZone(conf.sessionLocalTimeZone))

    val outputProjection = new InterpretedProjection(finalInput, child.output)

    // This new thread will consume the ScriptTransformation's input rows and write them to the
    // external process. That process's output will be read by this current thread.
    val writerThread = ScriptTransformationWriterThread(
      inputIterator.map(outputProjection),
      input.map(_.dataType),
      ioschema,
      outputStream,
      proc,
      stderrBuffer,
      TaskContext.get(),
      hadoopConf
    )

    val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    val outputIterator: Iterator[InternalRow] = new Iterator[InternalRow] {
      var curLine: String = null
      val mutableRow = new SpecificInternalRow(output.map(_.dataType))

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
        if (!ioschema.schemaLess) {
          new GenericInternalRow(
            prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
              .zip(output)
              .map { case (data, dataType) =>
                CatalystTypeConverters.convertToCatalyst(wrapper(data, dataType.dataType))
              })
        } else {
          new GenericInternalRow(
            prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"), 2)
              .zip(output)
              .map { case (data, dataType) =>
                CatalystTypeConverters.convertToCatalyst(wrapper(data, dataType.dataType))
              })
        }
      }
    }

    writerThread.start()

    outputIterator
  }
}

case class ScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    ioSchema: SparkScriptIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration)
  extends BaseScriptTransformationWriterThread {

  setDaemon(true)

  override def processRows(): Unit = {
    processRowsWithoutSerde()
  }
}

object SparkScriptIOSchema {
  def apply(input: ScriptInputOutputSchema): SparkScriptIOSchema = {
    SparkScriptIOSchema(
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
 * The wrapper class of Spark script transformation input and output schema properties
 */
case class SparkScriptIOSchema (
     inputRowFormat: Seq[(String, String)],
     outputRowFormat: Seq[(String, String)],
     inputSerdeClass: Option[String],
     outputSerdeClass: Option[String],
     inputSerdeProps: Seq[(String, String)],
     outputSerdeProps: Seq[(String, String)],
     recordReaderClass: Option[String],
     recordWriterClass: Option[String],
     schemaLess: Boolean) extends BaseScriptTransformIOSchema
