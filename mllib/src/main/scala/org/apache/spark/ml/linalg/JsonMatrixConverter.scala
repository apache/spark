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
package org.apache.spark.ml.linalg

import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse => parseJson, render}

import org.apache.spark.util.ArrayImplicits._

private[ml] object JsonMatrixConverter {

  /** Unique class name for identifying JSON object encoded by this class. */
  val className = "matrix"

  /**
   * Parses the JSON representation of a Matrix into a [[Matrix]].
   */
  def fromJson(json: String): Matrix = {
    implicit val formats: Formats = DefaultFormats
    val jValue = parseJson(json)
    (jValue \ "type").extract[Int] match {
      case 0 => // sparse
        val numRows = (jValue \ "numRows").extract[Int]
        val numCols = (jValue \ "numCols").extract[Int]
        val colPtrs = (jValue \ "colPtrs").extract[Seq[Int]].toArray
        val rowIndices = (jValue \ "rowIndices").extract[Seq[Int]].toArray
        val values = (jValue \ "values").extract[Seq[Double]].toArray
        val isTransposed = (jValue \ "isTransposed").extract[Boolean]
        new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
      case 1 => // dense
        val numRows = (jValue \ "numRows").extract[Int]
        val numCols = (jValue \ "numCols").extract[Int]
        val values = (jValue \ "values").extract[Seq[Double]].toArray
        val isTransposed = (jValue \ "isTransposed").extract[Boolean]
        new DenseMatrix(numRows, numCols, values, isTransposed)
      case _ =>
        throw new IllegalArgumentException(s"Cannot parse $json into a Matrix.")
    }
  }

  /**
   * Coverts the Matrix to a JSON string.
   */
  def toJson(m: Matrix): String = {
    m match {
      case SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed) =>
        val jValue = ("class" -> className) ~
          ("type" -> 0) ~
          ("numRows" -> numRows) ~
          ("numCols" -> numCols) ~
          ("colPtrs" -> colPtrs.toImmutableArraySeq) ~
          ("rowIndices" -> rowIndices.toImmutableArraySeq) ~
          ("values" -> values.toImmutableArraySeq) ~
          ("isTransposed" -> isTransposed)
        compact(render(jValue))
      case DenseMatrix(numRows, numCols, values, isTransposed) =>
        val jValue = ("class" -> className) ~
          ("type" -> 1) ~
          ("numRows" -> numRows) ~
          ("numCols" -> numCols) ~
          ("values" -> values.toImmutableArraySeq) ~
          ("isTransposed" -> isTransposed)
        compact(render(jValue))
      case _ =>
        throw new IllegalArgumentException(s"Unknown matrix type ${m.getClass}.")
    }
  }
}
