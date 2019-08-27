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

import java.io.{ByteArrayInputStream, DataInputStream}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Provides methods for converting compressed byte arrays back to UnsafeRows.
 * Additionally, can enforce a limit on the total, decoded size of all decoded UnsafeRows.
 * Enforcing the limit is controlled via a sql config and if it is turned on the encoder will
 * throw a SparkException when the limit is reached.
 */
private[spark] class SizeLimitingByteArrayDecoder(
     nFields: Int,
     conf: SparkConf,
     sqlConf: SQLConf) extends Logging {
  private var totalUncompressedResultSize = 0L
  private val maxResultSize = conf.get(config.MAX_RESULT_SIZE)
  private val limitUncompressedMaxResultSize = sqlConf.limitUncompressedResultSize

  /**
   * Decodes the byte arrays back to UnsafeRows and put them into buffer.
   */
  def decodeUnsafeRows(bytes: Array[Byte]): Iterator[InternalRow] = {
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(codec.compressedInputStream(bis))

    new Iterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()

      override def hasNext: Boolean = sizeOfNextRow >= 0

      override def next(): InternalRow = {
        ensureCanFetchMoreResults(sizeOfNextRow)
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }
    }
  }

  private def ensureCanFetchMoreResults(sizeOfNextRow: Int): Unit = {
    totalUncompressedResultSize += sizeOfNextRow
    if (limitUncompressedMaxResultSize && totalUncompressedResultSize > maxResultSize) {
      val msg = s"Total size of uncompressed results " +
        s"(${Utils.bytesToString(totalUncompressedResultSize)}) " +
        s"is bigger than ${config.MAX_RESULT_SIZE.key} (${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      throw new SparkException(msg)
    }
  }
}
