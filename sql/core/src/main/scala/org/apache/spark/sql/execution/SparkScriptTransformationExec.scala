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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.util.CircularBuffer

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 * @param child logical plan whose output is transformed.
 * @param ioschema the class set that defines how to handle input/output data.
 */
case class SparkScriptTransformationExec(
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: ScriptTransformationIOSchema)
  extends BaseScriptTransformationExec {

  override def processIterator(
      inputIterator: Iterator[InternalRow],
      hadoopConf: Configuration): Iterator[InternalRow] = {

    val (outputStream, proc, inputStream, stderrBuffer) = initProc

    val outputProjection = new InterpretedProjection(inputExpressionsWithoutSerde, child.output)

    // This new thread will consume the ScriptTransformation's input rows and write them to the
    // external process. That process's output will be read by this current thread.
    val writerThread = SparkScriptTransformationWriterThread(
      inputIterator.map(outputProjection),
      inputExpressionsWithoutSerde.map(_.dataType),
      ioschema,
      outputStream,
      proc,
      stderrBuffer,
      TaskContext.get(),
      hadoopConf
    )

    val outputIterator =
      createOutputIteratorWithoutSerde(writerThread, inputStream, proc, stderrBuffer)

    writerThread.start()

    outputIterator
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkScriptTransformationExec =
    copy(child = newChild)
}

case class SparkScriptTransformationWriterThread(
    iter: Iterator[InternalRow],
    inputSchema: Seq[DataType],
    ioSchema: ScriptTransformationIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext,
    conf: Configuration)
  extends BaseScriptTransformationWriterThread {

  override def processRows(): Unit = {
    processRowsWithoutSerde()
  }
}
