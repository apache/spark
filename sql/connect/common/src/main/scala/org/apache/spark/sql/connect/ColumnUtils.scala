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
package org.apache.spark.sql.connect

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.agnosticEncoderFor
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.MetadataBuilder

/**
 * Helper class for Column operations.
 */
private[connect] object ColumnUtils {
  private val CONNECT_INTERNAL = "__connect_internal"
  private val REGULAR_COL_ALIAS_METADATA = {
    new MetadataBuilder().putNull(CONNECT_INTERNAL).build()
  }

  /**
   * Select all Columns of a Dataset.
   */
  val all: Column = col("*")

  /**
   * Select a column of a Dataset by its position.
   */
  def getByOrdinal(ordinal: Int, planId: Option[Long] = None): Column = {
    require(ordinal >= 0)
    ConnectConversions.column { builder =>
      val b = builder.getGetColumnByOrdinalBuilder.setOrdinal(ordinal)
      planId.foreach(b.setPlanId)
    }
  }

  implicit class RichColumn(val column: Column) extends AnyVal {
    def asMetadataColumn(name: String): Column = {
      val metadata = new MetadataBuilder()
        .putString("__metadata_col", value = name)
        .putNull(CONNECT_INTERNAL)
        .build()
      column.as("", metadata)
    }

    def asRegularColumn(name: String): Column = {
      column.as(name, REGULAR_COL_ALIAS_METADATA)
    }
  }

  implicit class RichUdf(val udf: SparkUserDefinedFunction) extends AnyVal {
    def apply[E](ds: Dataset[E]): Column = {
      require(udf.inputEncoders.size == 1)
      require(udf.inputEncoders.head.isDefined)
      val encoder = agnosticEncoderFor(udf.inputEncoders.head.get)
      if (encoder.isStruct) {
        // The UDF is expecting a struct as its input so we create one here.
        udf(ds.wrapAll)
      } else {
        // The UDF is expecting a single value. In that case the convention
        // is to bind to the first column of the input.
        udf(ds.firstCol)
      }
    }

    def applyToAll(): Column = {
      udf.apply(udf.inputEncoders.map(_ => all): _*)
    }

    def canFlattenResult: Boolean = udf.outputEncoder.exists(e => agnosticEncoderFor(e).isStruct)

    def flattenResult(result: String): Column = col(result + ".*")
  }
}
