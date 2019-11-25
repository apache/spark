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

package org.apache.spark.api.r

import java.io._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

/**
 * A helper class to run R UDFs in Spark.
 */
private[spark] class RRunner[IN, OUT](
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    numPartitions: Int = -1,
    isDataFrame: Boolean = false,
    colNames: Array[String] = null,
    mode: Int = RRunnerModes.RDD)
  extends BaseRRunner[IN, OUT](
    func,
    deserializer,
    serializer,
    packageNames,
    broadcastVars,
    numPartitions,
    isDataFrame,
    colNames,
    mode) {

  protected def newReaderIterator(
      dataStream: DataInputStream, errThread: BufferedStreamThread): ReaderIterator = {
    new ReaderIterator(dataStream, errThread) {
      private val readData = numPartitions match {
        case -1 =>
          serializer match {
            case SerializationFormats.STRING => readStringData _
            case _ => readByteArrayData _
          }
        case _ => readShuffledData _
      }

      private def readShuffledData(length: Int): (Int, Array[Byte]) = {
        length match {
          case length if length == 2 =>
            val hashedKey = dataStream.readInt()
            val contentPairsLength = dataStream.readInt()
            val contentPairs = new Array[Byte](contentPairsLength)
            dataStream.readFully(contentPairs)
            (hashedKey, contentPairs)
          case _ => null
        }
      }

      private def readByteArrayData(length: Int): Array[Byte] = {
        length match {
          case length if length > 0 =>
            val obj = new Array[Byte](length)
            dataStream.readFully(obj)
            obj
          case _ => null
        }
      }

      private def readStringData(length: Int): String = {
        length match {
          case length if length > 0 =>
            SerDe.readStringBytes(dataStream, length)
          case _ => null
        }
      }

      /**
       * Reads next object from the stream.
       * When the stream reaches end of data, needs to process the following sections,
       * and then returns null.
       */
      override protected def read(): OUT = {
        try {
          val length = dataStream.readInt()

          length match {
            case SpecialLengths.TIMING_DATA =>
              // Timing data from R worker
              val boot = dataStream.readDouble - bootTime
              val init = dataStream.readDouble
              val broadcast = dataStream.readDouble
              val input = dataStream.readDouble
              val compute = dataStream.readDouble
              val output = dataStream.readDouble
              logInfo(
                ("Times: boot = %.3f s, init = %.3f s, broadcast = %.3f s, " +
                  "read-input = %.3f s, compute = %.3f s, write-output = %.3f s, " +
                  "total = %.3f s").format(
                  boot,
                  init,
                  broadcast,
                  input,
                  compute,
                  output,
                  boot + init + broadcast + input + compute + output))
              read()
            case length if length > 0 =>
              readData(length).asInstanceOf[OUT]
            case length if length == 0 =>
              // End of stream
              eos = true
              null.asInstanceOf[OUT]
          }
        } catch {
          case eof: EOFException =>
            throw new SparkException("R worker exited unexpectedly (cranshed)", eof)
        }
      }
    }
  }

  /**
   * Start a thread to write RDD data to the R process.
   */
  protected override def newWriterThread(
      output: OutputStream,
      iter: Iterator[IN],
      partitionIndex: Int): WriterThread = {
    new WriterThread(output, iter, partitionIndex) {

      /**
       * Writes input data to the stream connected to the R worker.
       */
      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        def writeElem(elem: Any): Unit = {
          if (deserializer == SerializationFormats.BYTE) {
            val elemArr = elem.asInstanceOf[Array[Byte]]
            dataOut.writeInt(elemArr.length)
            dataOut.write(elemArr)
          } else if (deserializer == SerializationFormats.ROW) {
            dataOut.write(elem.asInstanceOf[Array[Byte]])
          } else if (deserializer == SerializationFormats.STRING) {
            // write string(for StringRRDD)
            // scalastyle:off println
            printOut.println(elem)
            // scalastyle:on println
          }
        }

        for (elem <- iter) {
          elem match {
            case (key, innerIter: Iterator[_]) =>
              for (innerElem <- innerIter) {
                writeElem(innerElem)
              }
              // Writes key which can be used as a boundary in group-aggregate
              dataOut.writeByte('r')
              writeElem(key)
            case (key, value) =>
              writeElem(key)
              writeElem(value)
            case _ =>
              writeElem(elem)
          }
        }
      }
    }
  }
}
