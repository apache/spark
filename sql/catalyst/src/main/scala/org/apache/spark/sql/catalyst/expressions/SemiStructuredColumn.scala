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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

/**
 * Adds metadata to a column containing semi-structured data. This information can be passed down
 * to data sources for potential optimizations.
 */
trait SemiStructuredColumn extends UnaryExpression with ExpectsInputTypes with RuntimeReplaceable {

  /** The format of the semi-structured column, e.g. json, xml, avro, csv. */
  def format: String

  /**
   * Options that can be used to parse values from the column. Typically these will be options
   * that get passed to Spark's JSON or CSV parsers.
   */
  def formatOptions: Map[String, String]

  /** Encode the column information as metadata */
  final def toMetadata: Metadata = {
    val options = new MetadataBuilder
    formatOptions.foreach { case (key, value) =>
      options.putString(key, value)
    }

    new MetadataBuilder()
      .putString(SemiStructuredColumn.FORMAT_KEY, format)
      .putMetadata(SemiStructuredColumn.FORMAT_OPTIONS_KEY, options.build())
      .build()
  }
}

object SemiStructuredColumn {

  val FORMAT_KEY = "format"
  val FORMAT_OPTIONS_KEY = "options"
}
