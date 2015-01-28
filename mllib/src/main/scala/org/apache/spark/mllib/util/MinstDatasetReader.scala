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

package org.apache.spark.mllib.util

import java.io.{Closeable, DataInputStream, FileInputStream, IOException}
import java.util.zip.GZIPInputStream

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{DenseVector => SDV, Vector => SV}

case class MinstItem(label: Int, data: Array[Int]) {
  def binaryVector: SV = {
    new SDV(data.map { i =>
      if (i > 30) {
        1D
      } else {
        0D
      }
    })
  }
}

class MinstDatasetReader(labelsFile: String, imagesFile: String)
  extends java.util.Iterator[MinstItem] with Closeable with Logging {

  val labelsBuf: DataInputStream = new DataInputStream(new GZIPInputStream(
    new FileInputStream(labelsFile)))
  var magic = labelsBuf.readInt()
  val labelCount = labelsBuf.readInt()
  logInfo(s"Labels magic=$magic count= $labelCount")

  val imagesBuf: DataInputStream = new DataInputStream(new GZIPInputStream(
    new FileInputStream(imagesFile)))
  magic = imagesBuf.readInt()
  val imageCount = imagesBuf.readInt()
  val rows = imagesBuf.readInt()
  val cols = imagesBuf.readInt()
  logInfo(s"Images magic=$magic count=$imageCount rows=$rows cols=$cols")
  assert(imageCount == labelCount)

  var current = 0

  override def next(): MinstItem = {
    try {
      val data = new Array[Int](rows * cols)
      for (i <- 0 until data.length) {
        data(i) = imagesBuf.readUnsignedByte()
      }
      val label = labelsBuf.readUnsignedByte()
      MinstItem(label, data)
    } catch {
      case e: IOException =>
        current = imageCount
        throw e
    }
    finally {
      current += 1
    }
  }

  override def hasNext = current < imageCount

  override def close: Unit = {
    imagesBuf.close()
    labelsBuf.close()
  }

  override def remove {
    throw new UnsupportedOperationException("remove")
  }
}
