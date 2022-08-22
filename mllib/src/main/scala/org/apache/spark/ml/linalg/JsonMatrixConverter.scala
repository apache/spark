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

import org.apache.spark.util.JacksonUtils

private[ml] object JsonMatrixConverter {

  /** Unique class name for identifying JSON object encoded by this class. */
  val className = "matrix"

  /**
   * Parses the JSON representation of a Matrix into a [[Matrix]].
   */
  def fromJson(json: String): Matrix = {
    val jsonNode = JacksonUtils.readTree(json)
    jsonNode.get("type").intValue() match {
      case 0 => // sparse
        val numRows = jsonNode.get("numRows").intValue()
        val numCols = jsonNode.get("numCols").intValue()
        val colPtrs = JacksonUtils.treeToValue[Seq[Int]](jsonNode.get("colPtrs")).toArray
        val rowIndices = JacksonUtils.treeToValue[Seq[Int]](jsonNode.get("rowIndices")).toArray
        val values = JacksonUtils.treeToValue[Seq[Double]](jsonNode.get("values")).toArray
        val isTransposed = jsonNode.get("isTransposed").booleanValue()
        new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
      case 1 => // dense
        val numRows = jsonNode.get("numRows").intValue()
        val numCols = jsonNode.get("numCols").intValue()
        val values = JacksonUtils.treeToValue[Seq[Double]](jsonNode.get("values")).toArray
        val isTransposed = jsonNode.get("isTransposed").booleanValue()
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
        val sparse = JacksonUtils.createObjectNode
        sparse.put("class", className)
        sparse.put("type", 0)
        sparse.put("numRows", numRows)
        sparse.put("numCols", numCols)
        sparse.putPOJO("colPtrs", colPtrs.toSeq)
        sparse.putPOJO("rowIndices", rowIndices.toSeq)
        sparse.putPOJO("values", values.toSeq)
        sparse.put("isTransposed", isTransposed)
        JacksonUtils.writeValueAsString(sparse)
      case DenseMatrix(numRows, numCols, values, isTransposed) =>
        val dense = JacksonUtils.createObjectNode
        dense.put("class", className)
        dense.put("type", 1)
        dense.put("numRows", numRows)
        dense.put("numCols", numCols)
        dense.putPOJO("values", values.toSeq)
        dense.put("isTransposed", isTransposed)
        JacksonUtils.writeValueAsString(dense)
      case _ =>
        throw new IllegalArgumentException(s"Unknown matrix type ${m.getClass}.")
    }
  }
}
