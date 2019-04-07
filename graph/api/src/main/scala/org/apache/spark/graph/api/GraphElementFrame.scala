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

package org.apache.spark.graph.api

import org.apache.spark.graph.api.GraphElementFrame.encodeIdColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object GraphElementFrame {

  def encodeIdColumns(df: DataFrame, idColumnNames: String*): DataFrame = {
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

/**
  * A [[PropertyGraph]] is created from GraphElementFrames.
  *
  * Wraps a [[DataFrame]] and describes how it maps to graph elements (i.e. nodes or relationships).
  */
trait GraphElementFrame {

  /**
    * Initial [[DataFrame]] that can still contain unmapped columns and ID columns that are not of type BinaryType.
    * The columns are ordered arbitrarily.
    */
  def initialDf: DataFrame

  /**
    * [[DataFrame]] that contains only mapped element data. Each row represents a graph element.
    * Columns in 'initialDf' that do not have BinaryType are converted to BinaryType.
    * ID columns are first, property columns are sorted alphabetically.
    */
  val df: DataFrame = {
    val mappedColumnNames = idColumns ++ properties.values.toSeq.sorted
    val mappedDf = if (mappedColumnNames == initialDf.columns.toSeq) {
      initialDf
    } else {
      initialDf.select(mappedColumnNames.map(initialDf.col): _*)
    }
    if (idColumns.forall(idColumn => initialDf.schema(idColumn).dataType == BinaryType)) {
      mappedDf
    } else {
      encodeIdColumns(mappedDf, idColumns: _*)
    }
  }

  /**
    * Name of the column that contains the graph element identifier.
    *
    * @note Column values need to be of [[org.apache.spark.sql.types.BinaryType]].
    */
  def idColumn: String

  /**
    * Mapping from graph element property keys to the columns that contain the corresponding property values.
    */
  def properties: Map[String, String]

  protected def idColumns: Seq[String]

}

object NodeFrame {

  /**
    * Describes how to map an input [[DataFrame]] to nodes.
    *
    * All columns apart from the given `idColumn` are mapped to node properties.
    *
    * @param initialDf [[DataFrame]] containing a single node in each row
    * @param idColumn  column that contains the node identifier
    * @param labelSet  labels that are assigned to all nodes
    */
  def apply(
    initialDf: DataFrame,
    idColumn: String,
    labelSet: Set[String] = Set.empty
  ): NodeFrame = {
    val properties = (initialDf.columns.toSet - idColumn)
      .map(columnName => columnName -> columnName).toMap
    NodeFrame(initialDf, idColumn, labelSet, properties)
  }

}

/**
  * Describes how to map an input [[DataFrame]] to nodes.
  *
  * Each row in the [[DataFrame]] represents a node which has exactly the labels defined by the
  * given label set.
  *
  * @param initialDf  [[DataFrame]] containing a single node in each row
  * @param idColumn   column that contains the node identifier
  * @param labelSet   labels that are assigned to all nodes
  * @param properties mapping from property keys to corresponding columns
  */
case class NodeFrame(
  initialDf: DataFrame,
  idColumn: String,
  labelSet: Set[String],
  properties: Map[String, String]
) extends GraphElementFrame {

  override protected def idColumns: Seq[String] = Seq(idColumn)

}

object RelationshipFrame {

  /**
    * Describes how to map an input [[DataFrame]] to relationships.
    *
    * All columns apart from the given identifier columns are mapped to relationship properties.
    *
    * @param initialDf        [[DataFrame]] containing a single relationship in each row
    * @param idColumn         column that contains the relationship identifier
    * @param sourceIdColumn   column that contains the source node identifier of the relationship
    * @param targetIdColumn   column that contains the target node identifier of the relationship
    * @param relationshipType relationship type that is assigned to all relationships
    */
  def apply(
    initialDf: DataFrame,
    idColumn: String,
    sourceIdColumn: String,
    targetIdColumn: String,
    relationshipType: String
  ): RelationshipFrame = {
    val properties = (initialDf.columns.toSet - idColumn - sourceIdColumn - targetIdColumn)
      .map(columnName => columnName -> columnName).toMap
    RelationshipFrame(initialDf, idColumn, sourceIdColumn, targetIdColumn, relationshipType, properties)
  }

}

/**
  * Describes how to map an input [[DataFrame]] to relationships.
  *
  * Each row in the [[DataFrame]] represents a relationship with the given relationship type.
  *
  * @param initialDf        [[DataFrame]] containing a single relationship in each row
  * @param idColumn         column that contains the relationship identifier
  * @param sourceIdColumn   column that contains the source node identifier of the relationship
  * @param targetIdColumn   column that contains the target node identifier of the relationship
  * @param relationshipType relationship type that is assigned to all relationships
  * @param properties       mapping from property keys to corresponding columns
  */
case class RelationshipFrame(
  initialDf: DataFrame,
  idColumn: String,
  sourceIdColumn: String,
  targetIdColumn: String,
  relationshipType: String,
  properties: Map[String, String]
) extends GraphElementFrame {

  override protected def idColumns: Seq[String] = Seq(idColumn, sourceIdColumn, targetIdColumn)

}
