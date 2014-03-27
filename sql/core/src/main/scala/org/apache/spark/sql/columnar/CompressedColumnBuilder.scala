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

package org.apache.spark.sql.columnar

import org.apache.spark.sql.{Logging, Row}
import org.apache.spark.sql.catalyst.types.NativeType

private[sql] trait CompressedColumnBuilder[T <: NativeType] extends ColumnBuilder with Logging {
  this: BasicColumnBuilder[T, T#JvmType] =>

  val compressionSchemes = Seq(new CompressionAlgorithm.Noop)
    .filter(_.supports(columnType))

  def isWorthCompressing(scheme: CompressionAlgorithm) = {
    scheme.compressionRatio < 0.8
  }

  abstract override def gatherStats(row: Row, ordinal: Int) {
    compressionSchemes.foreach {
      val field = columnType.getField(row, ordinal)
      _.gatherCompressibilityStats(field, columnType)
    }

    super.gatherStats(row, ordinal)
  }

  abstract override def build() = {
    val rawBuffer = super.build()

    if (compressionSchemes.isEmpty) {
      logger.info(s"Compression scheme chosen for [$columnName] is ${CompressionType.Noop}")
      new CompressionAlgorithm.Noop().compress(rawBuffer, columnType)
    } else {
      val candidateScheme = compressionSchemes.minBy(_.compressionRatio)

      logger.info(
        s"Compression scheme chosen for [$columnName] is ${candidateScheme.compressionType} " +
          s"ration ${candidateScheme.compressionRatio}")

      if (isWorthCompressing(candidateScheme)) {
        candidateScheme.compress(rawBuffer, columnType)
      } else {
        new CompressionAlgorithm.Noop().compress(rawBuffer, columnType)
      }
    }
  }
}
