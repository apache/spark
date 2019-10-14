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
 *
 */

package org.apache.spark.cypher.conversions

import org.apache.spark.graph.api.GraphElementDataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

object GraphElementFrameConversions {

  def normalizeDf(frame: GraphElementDataset): Dataset[Row] = {
    val mappedColumnNames = frame.idColumns.toSeq ++ frame.propertyColumns.values.toSeq.sorted
    val mappedDf = if (mappedColumnNames == frame.ds.columns.toSeq) {
      frame.ds
    } else {
      frame.ds.select(mappedColumnNames.map(frame.ds.col): _*)
    }
    if (frame.idColumns.forall(idColumn => frame.ds.schema(idColumn).dataType == BinaryType)) {
      mappedDf
    } else {
      encodeIdColumns(mappedDf, frame.idColumns: _*)
    }
  }

  private def encodeIdColumns(ds: Dataset[Row], idColumnNames: String*): Dataset[Row] = {
    val encodedIdCols = idColumnNames.map { idColumnName =>
      val col = ds.col(idColumnName)
      ds.schema(idColumnName).dataType match {
        case BinaryType => col
        case StringType | ByteType | ShortType | IntegerType | LongType => col.cast(BinaryType)
        // TODO: Constrain to types that make sense as IDs
        case _ => col.cast(StringType).cast(BinaryType)
      }
    }
    val remainingColumnNames = ds.columns.filterNot(idColumnNames.contains)
    val remainingCols = remainingColumnNames.map(ds.col)
    ds.select(encodedIdCols ++ remainingCols: _*)
  }

}

