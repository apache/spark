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

package org.apache.spark.sql.execution.python

import java.io._
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark._
import org.apache.spark.api.python._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.internal.SQLConf

/**
 * A helper class to run Python UDFs in Spark.
 */
abstract class BasePythonUDFRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
  extends BasePythonRunner[Array[Byte], Array[Byte]](
    funcs.map(_._1), evalType, argOffsets, jobArtifactUUID, pythonMetrics) {

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(
      funcs.head._1.funcs.head.pythonExec)

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled

  override val bufferSize: Int = SQLConf.get.getConf(SQLConf.PYTHON_UDF_BUFFER_SIZE)

  protected def writeUDF(dataOut: DataOutputStream): Unit

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[Array[Byte]],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        writeUDF(dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        val startData = dataOut.size()
        val wroteData = PythonRDD.writeNextElementToStream(inputIterator, dataOut)
        if (!wroteData) {
          // Reached the end of input.
          dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
        }
        val deltaData = dataOut.size() - startData
        pythonMetrics("pythonDataSent") += deltaData
        wroteData
      }
    }
  }

  protected override def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[Array[Byte]] = {
    new ReaderIterator(
      stream, writer, startTime, env, worker, pid, releasedOrClosed, context) {

      protected override def read(): Array[Byte] = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          stream.readInt() match {
            case length if length >= 0 =>
              val obj = PythonWorkerUtils.readBytes(length, stream)
              pythonMetrics("pythonDataReceived") += length
              obj
            case SpecialLengths.TIMING_DATA =>
              handleTimingData()
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              throw handlePythonException()
            case SpecialLengths.END_OF_DATA_SECTION =>
              handleEndOfDataSection()
              null
          }
        } catch handleException
      }
    }
  }
}

class PythonUDFRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    profiler: Option[String])
  extends BasePythonUDFRunner(funcs, evalType, argOffsets, pythonMetrics, jobArtifactUUID) {

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, profiler)
  }
}

class PythonUDFWithNamedArgumentsRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argMetas: Array[Array[ArgumentMetadata]],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    profiler: Option[String])
  extends BasePythonUDFRunner(
    funcs, evalType, argMetas.map(_.map(_.offset)), pythonMetrics, jobArtifactUUID) {

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    PythonUDFRunner.writeUDFs(dataOut, funcs, argMetas, profiler)
  }
}

object PythonUDFRunner {

  def writeUDFs(
      dataOut: DataOutputStream,
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argOffsets: Array[Array[Int]],
      profiler: Option[String]): Unit = {
    profiler match {
      case Some(p) =>
        dataOut.writeBoolean(true)
        PythonWorkerUtils.writeUTF(p, dataOut)
      case _ => dataOut.writeBoolean(false)
    }
    dataOut.writeInt(funcs.length)
    funcs.zip(argOffsets).foreach { case ((chained, resultId), offsets) =>
      dataOut.writeInt(offsets.length)
      offsets.foreach { offset =>
        dataOut.writeInt(offset)
      }
      dataOut.writeInt(chained.funcs.length)
      chained.funcs.foreach { f =>
        PythonWorkerUtils.writePythonFunction(f, dataOut)
      }
      if (profiler.isDefined) {
        dataOut.writeLong(resultId)
      }
    }
  }

  def writeUDFs(
      dataOut: DataOutputStream,
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      profiler: Option[String]): Unit = {
    profiler match {
      case Some(p) =>
        dataOut.writeBoolean(true)
        PythonWorkerUtils.writeUTF(p, dataOut)
      case _ => dataOut.writeBoolean(false)
    }
    dataOut.writeInt(funcs.length)
    funcs.zip(argMetas).foreach { case ((chained, resultId), metas) =>
      dataOut.writeInt(metas.length)
      metas.foreach {
        case ArgumentMetadata(offset, name) =>
          dataOut.writeInt(offset)
          name match {
            case Some(name) =>
              dataOut.writeBoolean(true)
              PythonWorkerUtils.writeUTF(name, dataOut)
            case _ =>
              dataOut.writeBoolean(false)
          }
      }
      dataOut.writeInt(chained.funcs.length)
      chained.funcs.foreach { f =>
        PythonWorkerUtils.writePythonFunction(f, dataOut)
      }
      if (profiler.isDefined) {
        dataOut.writeLong(resultId)
      }
    }
  }
}
