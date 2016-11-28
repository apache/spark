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

package org.apache.spark.sql.streaming

import scala.util.control.NonFatal

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Reports metrics on data being read from a given streaming source.
 *
 * @param description Description of the source.
 * @param startOffset The starting offset for data being read.
 * @param endOffset The ending offset for data being read.
 * @param numInputRows The number of records read from this source.
 * @param inputRowsPerSecond The rate at which data is arriving from this source.
 * @param processedRowsPerSecond The rate at which data from this source is being procressed by
 *                                  Spark.
 * @since 2.1.0
 */
@Experimental
class SourceProgress protected[sql](
    val description: String,
    val startOffset: String,
    val endOffset: String,
    val numInputRows: Long,
    val inputRowsPerSecond: Double,
    val processedRowsPerSecond: Double) {

  /** The compact JSON representation of this status. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this status. */
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = prettyJson

  private[sql] def jsonValue: JValue = {
    ("description" -> JString(description)) ~
    ("startOffset" -> tryParse(startOffset)) ~
    ("endOffset" -> tryParse(endOffset)) ~
    ("numInputRows" -> JInt(numInputRows)) ~
    ("inputRowsPerSecond" -> JDouble(inputRowsPerSecond)) ~
    ("processedRowsPerSecond" -> JDouble(processedRowsPerSecond))
  }

  private def tryParse(json: String) = try {
    parse(json)
  } catch {
    case NonFatal(e) => JString(json)
  }
}
