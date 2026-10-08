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

package org.apache.spark.graphframes.propertygraph.property

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.InvalidPropertyGroupException
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sha2
import org.apache.spark.sql.types._

/**
 * Represents a logical group of edges in a property graph with associated metadata and data.
 *
 * EdgePropertyGroup encapsulates edge data stored in a DataFrame along with metadata describing
 * how to interpret the data as graph edges. Each edge group has:
 *
 *   - A unique name identifier
 *   - DataFrame containing the actual edge data
 *   - Source and destination vertex property groups
 *   - Direction flag indicating if edges are directed or undirected
 *   - Column names specifying source vertex, destination vertex and edge weight columns
 *
 * The class validates that required columns exist in the provided DataFrame on creation. Required
 * columns are:
 *   - Source vertex column
 *   - Destination vertex column
 *   - Weight column
 *
 * @param name
 *   Unique identifier for this edge property group
 * @param data
 *   DataFrame containing the edge data with required columns
 * @param srcPropertyGroup
 *   Source vertex property group
 * @param dstPropertyGroup
 *   Destination vertex property group
 * @param isDirected
 *   Whether edges should be treated as directed (true) or undirected (false)
 * @param srcColumnName
 *   Name of the source vertex column in the data
 * @param dstColumnName
 *   Name of the destination vertex column in the data
 * @param weightColumnName
 *   Name of the edge weight column in the data
 * @note
 *   When edges from different groups are combined into a GraphFrame, their SRCs and DSTs are
 *   hashed with the group name to prevent collisions in the same way as ID of the corresponded
 *   vertex group is hashed.
 */
case class EdgePropertyGroup(
    name: String,
    data: DataFrame,
    srcPropertyGroup: VertexPropertyGroup,
    dstPropertyGroup: VertexPropertyGroup,
    isDirected: Boolean,
    srcColumnName: String,
    dstColumnName: String,
    weightColumnName: String)
    extends PropertyGroup {

  override protected def validate(): this.type = {
    if (!data.columns.contains(srcColumnName)) {
      throw new InvalidPropertyGroupException(
        s"source column $srcColumnName does not exist, existed columns " +
          s"[${data.columns.mkString(", ")}]")
    }
    if (!data.columns.contains(dstColumnName)) {
      throw new InvalidPropertyGroupException(
        s"dest column $dstColumnName does not exist, existed columns " +
          s"[${data.columns.mkString(", ")}]")
    }
    if (!data.columns.contains(weightColumnName)) {
      throw new InvalidPropertyGroupException(
        s"weight column $weightColumnName does not exist, existed columns " +
          s"[${data.columns.mkString(", ")}]")
    }
    val weightColumnType = data.schema(weightColumnName).dataType
    if (!weightColumnType.isInstanceOf[NumericType]) {
      throw new InvalidPropertyGroupException(
        s"weight column $weightColumnName must be numeric type, but was $weightColumnType")
    }

    this
  }

  private def hashSrcEdge: Column = if (srcPropertyGroup.applyMaskOnId) {
    concat(lit(srcPropertyGroup.name), sha2(col(srcColumnName).cast(StringType), 256))
  } else {
    col(srcColumnName).cast(StringType)
  }

  private def hashDstEdge: Column = if (dstPropertyGroup.applyMaskOnId) {
    concat(lit(dstPropertyGroup.name), sha2(col(dstColumnName).cast(StringType), 256))
  } else {
    col(dstColumnName).cast(StringType)
  }

  override protected[graphframes] def getData(filter: Column): DataFrame = {
    val filteredData = data.filter(filter)

    val baseEdges = filteredData.select(
      hashSrcEdge.alias(GraphFrame.SRC),
      hashDstEdge.alias(GraphFrame.DST),
      col(weightColumnName).alias(GraphFrame.WEIGHT))

    if (isDirected) {
      baseEdges
    } else {
      baseEdges.union(
        baseEdges.select(
          col(GraphFrame.DST).as(GraphFrame.SRC),
          col(GraphFrame.SRC).as(GraphFrame.DST),
          col(GraphFrame.WEIGHT).alias(GraphFrame.WEIGHT)))
    }
  }
}

object EdgePropertyGroup {
  def apply(
      name: String,
      data: DataFrame,
      srcPropertyGroup: VertexPropertyGroup,
      dstPropertyGroup: VertexPropertyGroup,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String,
      weightColumnName: String): EdgePropertyGroup = {
    new EdgePropertyGroup(
      name,
      data,
      srcPropertyGroup,
      dstPropertyGroup,
      isDirected,
      srcColumnName,
      dstColumnName,
      weightColumnName).validate()
  }

  def apply(
      name: String,
      data: DataFrame,
      srcPropertyGroup: VertexPropertyGroup,
      dstPropertyGroup: VertexPropertyGroup,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String,
      weightColumn: Column): EdgePropertyGroup = {
    val dataWithWeight = data.withColumn(GraphFrame.WEIGHT, weightColumn)
    apply(
      name,
      dataWithWeight,
      srcPropertyGroup,
      dstPropertyGroup,
      isDirected,
      srcColumnName,
      dstColumnName,
      GraphFrame.WEIGHT)
  }
}
