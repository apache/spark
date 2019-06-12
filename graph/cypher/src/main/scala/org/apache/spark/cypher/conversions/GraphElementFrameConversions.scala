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

import org.apache.spark.graph.api.GraphElementFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BinaryType, ByteType, IntegerType, LongType, ShortType, StringType}

object GraphElementFrameConversions {

  def normalizeDf(frame: GraphElementFrame): DataFrame = {
    val mappedColumnNames = frame.idColumns ++ frame.properties.values.toSeq.sorted
    val mappedDf = if (mappedColumnNames == frame.df.columns.toSeq) {
      frame.df
    } else {
      frame.df.select(mappedColumnNames.map(frame.df.col): _*)
    }
    if (frame.idColumns.forall(idColumn => frame.df.schema(idColumn).dataType == BinaryType)) {
      mappedDf
    } else {
      encodeIdColumns(mappedDf, frame.idColumns: _*)
    }
  }

  private def encodeIdColumns(df: DataFrame, idColumnNames: String*): DataFrame = {
    val encodedIdCols = idColumnNames.map { idColumnName =>
      val col = df.col(idColumnName)
      df.schema(idColumnName).dataType match {
        case BinaryType => col
        case StringType | ByteType | ShortType | IntegerType | LongType => col.cast(BinaryType)
        // TODO: Constrain to types that make sense as IDs
        case _ => col.cast(StringType).cast(BinaryType)
      }
    }
    val remainingColumnNames = df.columns.filterNot(idColumnNames.contains)
    val remainingCols = remainingColumnNames.map(df.col)
    df.select(encodedIdCols ++ remainingCols: _*)
  }

}

