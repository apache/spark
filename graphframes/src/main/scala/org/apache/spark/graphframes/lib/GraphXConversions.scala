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

package org.apache.spark.graphframes.lib

import scala.reflect.runtime.universe._

import org.apache.spark.graphframes.{GraphFrame, NoSuchVertexException}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Convenience functions to map GraphX graphs to GraphFrames, checking for the types expected by
 * GraphX.
 */
private[graphframes] object GraphXConversions {

  import GraphFrame._

  /** Indicates if T is a Unit type */
  private def isUnitType[T: TypeTag]: Boolean = {
    val t = typeOf[T]
    typeOf[Unit] =:= t
  }

  /** Indicates if T is a Product type */
  private def isProductType[T: TypeTag]: Boolean = {
    val t = typeOf[T]
    // See https://stackoverflow.com/questions/21209006/
    // how-to-check-if-reflected-type-represents-a-tuple
    t.typeSymbol.fullName.startsWith("scala.Tuple")
  }

  /** See [[GraphFrame.fromGraphX()]] for documentation */
  def fromGraphX[V: TypeTag, E: TypeTag](
      originalGraph: GraphFrame,
      graph: Graph[V, E],
      vertexNames: Seq[String] = Nil,
      edgeNames: Seq[String] = Nil): GraphFrame = {
    val spark = originalGraph.spark
    // catalyst does not like the unit type, make sure to filter it first.
    val vertexDF: DataFrame = if (isUnitType[V]) {
      val vertexData = graph.vertices.map { case (vid, _) => Tuple1(vid) }
      spark.createDataFrame(vertexData).toDF(LONG_ID)
    } else if (isProductType[V]) {
      val vertexData = graph.vertices.map { case (vid, data) => (vid, data) }
      val vertexDF0 = spark.createDataFrame(vertexData).toDF(LONG_ID, GX_ATTR)
      renameStructFields(vertexDF0, GX_ATTR, vertexNames)
    } else {
      // Assume it is just one field, and pack it in a tuple to have a structure.
      val vertexData = graph.vertices.map { case (vid, data) => (vid, Tuple1(data)) }
      val vertexDF0 = spark.createDataFrame(vertexData).toDF(LONG_ID, GX_ATTR)
      renameStructFields(vertexDF0, GX_ATTR, vertexNames)
    }

    val edgeDF: DataFrame = if (isUnitType[E]) {
      val edgeData = graph.edges.map { e => (e.srcId, e.dstId) }
      spark.createDataFrame(edgeData).toDF(LONG_SRC, LONG_DST)
    } else if (isProductType[E]) {
      val edgeData = graph.edges.map { e => (e.srcId, e.dstId, e.attr) }
      val edgeDF0 = spark.createDataFrame(edgeData).toDF(LONG_SRC, LONG_DST, GX_ATTR)
      renameStructFields(edgeDF0, GX_ATTR, edgeNames)
    } else {
      val edgeData = graph.edges.map { e => (e.srcId, e.dstId, Tuple1(e.attr)) }
      val edgeDF0 = spark.createDataFrame(edgeData).toDF(LONG_SRC, LONG_DST, GX_ATTR)
      renameStructFields(edgeDF0, GX_ATTR, edgeNames)
    }
    fromGraphX(originalGraph, vertexDF, edgeDF)
  }

  /**
   * Given the name of a column (assumed to contain a struct), renames all the fields of this
   * struct.
   *
   * @param structName
   *   Struct name whose fields will be renamed. This method assumes this field exists and will
   *   not check for errors.
   * @param fieldNames
   *   List of new field names corresponding to all fields in the struct col.
   */
  private[lib] def renameStructFields(
      df: DataFrame,
      structName: String,
      fieldNames: Seq[String]): DataFrame = {
    // TODO(tjh) this looses metadata and other info in the process
    val origSubfields = colStar(df, structName).map(col)
    val renamedSubfields = origSubfields.zip(fieldNames).map { case (orig, newName) =>
      orig.as(newName)
    }
    val otherFields = df.schema.fieldNames.filter(_ != structName).map(quote).map(col)
    if (renamedSubfields.isEmpty) {
      // Do not attempt to add an empty structure.
      df.select(otherFields.toSeq: _*)
    } else {
      val renamedStruct = struct(renamedSubfields.toSeq: _*).as(structName)
      df.select((renamedStruct +: otherFields).toSeq: _*)
    }
  }

  private def drop(df: DataFrame, cols: String*): DataFrame = {
    val remainingCols = df.schema.map(_.name).filterNot(cols.contains).map(quote).map(n => df(n))
    df.select(remainingCols: _*)
  }

  /** Unpacks all struct fields and leaves other fields alone */
  private def unpackStructFields(df: DataFrame): DataFrame = {
    val cols = df.schema.flatMap {
      case StructField(fname, dt: StructType, nullable @ _, meta @ _) =>
        dt.iterator.map(sub => col(quote(fname, sub.name)).as(sub.name))
      case f => Seq(col(quote(f.name)))
    }
    df.select(cols: _*)
  }

  /**
   * Joins all the data from the original columns against the new data. Assumes the columns are
   * not going to conflict.
   *
   * @param gxVertexData
   *   DataFrame with column [[LONG_ID]] and optionally column [[GX_ATTR]]
   * @param gxEdgeData
   *   DataFrame with columns [[LONG_DST]], [[LONG_SRC]] and optionally column [[GX_ATTR]]
   */
  private def fromGraphX(
      originalGraph: GraphFrame,
      gxVertexData: DataFrame,
      gxEdgeData: DataFrame): GraphFrame = {
    // The ID is going to be unpacked from the attr field
    val packedVertices = drop(originalGraph.indexedVertices, ID).join(gxVertexData, LONG_ID)
    val vertexDF = unpackStructFields(drop(packedVertices, LONG_ID))

    val packedEdges = {
      val indexedEdges = originalGraph.indexedEdges
      // Handle 2 cases: GraphX edge has attr, or not.
      val hasGxAttr = gxEdgeData.schema.exists(_.name == GX_ATTR)
      val gxCol = if (hasGxAttr) { Seq(col(GX_ATTR)) }
      else { Seq() }
      val sel1 = Seq(col(LONG_SRC), col(LONG_DST)) ++ gxCol
      val gxe = gxEdgeData.select(sel1: _*)
      val sel3 = Seq(col(ATTR)) ++ gxCol
      // TODO: CHECK IN UNIT TESTS: Drop the src and dst columns from the index, they are already
      // in the attributes and will be unpacked with the rest of the user columns.
      // TODO(tjh) 2-step join?
      gxe
        .join(
          indexedEdges.select(indexedEdges(LONG_SRC), indexedEdges(LONG_DST), indexedEdges(ATTR)),
          (gxe(LONG_SRC) === indexedEdges(LONG_SRC)) && (gxe(LONG_DST) === indexedEdges(
            LONG_DST)))
        .select(sel3: _*)
    }
    val edgeDF = unpackStructFields(drop(packedEdges, LONG_SRC, LONG_DST))

    GraphFrame(vertexDF, edgeDF)
  }

  /**
   * Given a graph and an object, gets the the corresponding integral id in the internal
   * representation.
   */
  private[graphframes] def integralId(graph: GraphFrame, vertexId: Any): Long = {
    // Check if we can directly convert it
    vertexId match {
      case x: Int => return x.toLong
      case x: Long => return x.toLong
      case x: Short => return x.toLong
      case x: Byte => return x.toLong
      case _ =>
    }
    // If the vertex is a non-integral type such as a String, we need to use the translation table.
    val longIdRow: Array[Row] = graph.indexedVertices
      .filter(col(GraphFrame.ID) === vertexId)
      .select(GraphFrame.LONG_ID)
      .take(1)
    if (longIdRow.isEmpty) {
      throw new NoSuchVertexException(
        "GraphFrame algorithm given vertex ID which does not exist" +
          s" in Graph. Vertex ID $vertexId not contained in $graph")
    }
    // TODO(tjh): could do more informative message
    longIdRow.head.getLong(0)
  }
}

private[lib] trait Arguments {
  private[lib] def check[A](a: Option[A], name: String): A = {
    a.getOrElse(throw new IllegalArgumentException(s"Param $name is required."))
  }
}
