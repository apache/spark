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

package org.apache.spark.sql.pipelines.graph

import java.util

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.pipelines.common.DatasetType
import org.apache.spark.sql.pipelines.util.SchemaInferenceUtils
import org.apache.spark.sql.types.StructType

/** An element in a [[DataflowGraph]]. */
trait GraphElement {

  /**
   * Contains provenance to tie back this GraphElement to the user code that defined it.
   *
   * This must be set when a [[GraphElement]] is directly created by some user code.
   * Subsequently, this initial origin must be propagated as is without modification.
   * If this [[GraphElement]] is copied or converted to a different type, then this origin must be
   * copied as is.
   */
  def origin: QueryOrigin

  protected def spark: SparkSession = SparkSession.getActiveSession.get

  /** Returns the unique identifier for this [[GraphElement]]. */
  def identifier: TableIdentifier

  /**
   * Returns a user-visible name for the element.
   */
  def displayName: String = identifier.unquotedString
}

/**
 * Specifies an input that can be referenced by another Dataset's query.
 */
trait Input extends GraphElement {

  /**
   * Returns a DataFrame that is a result of loading data from this [[Input]].
   * @return Streaming or batch DataFrame of this Input's data.
   */
  def load: DataFrame
}

/**
 * Represents a node in a [[DataflowGraph]] that can be written to by a [[Flow]].
 * Must be backed by a file source.
 */
sealed trait Output {}

/**
 * A type of [[Output]] that represents a materialized dataset in a [[DataflowGraph]].
 */
sealed trait Dataset extends Output {
  /**
   * Normalized storage location used for storing materializations for this [[Output]].
   * If None, it means this [[Output]] has not been normalized yet.
   */
  def normalizedPath: Option[String]

  /** Return whether the storage location for this [[Output]] has been normalized. */
  final def normalized: Boolean = normalizedPath.isDefined

  /**
   * Return the normalized storage location for this [[Output]] and throw if the
   * storage location has not been normalized.
   */
  @throws[SparkException]
  def path: String
}

/** A type of [[Input]] where data is loaded from a table. */
sealed trait TableElement extends GraphElement {

  /** The user-specified schema for this table. */
  def specifiedSchema: Option[StructType]
}

/**
 * A table representing a materialized dataset in a [[DataflowGraph]].
 *
 * @param identifier The identifier of this table within the graph.
 * @param specifiedSchema The user-specified schema for this table.
 * @param partitionCols What columns the table should be partitioned by when materialized.
 * @param clusterCols What columns the table should be clustered by when materialized.
 * @param normalizedPath Normalized storage location for the table based on the user-specified table
 *                       path (if not defined, we will normalize a managed storage path for it).
 * @param properties Table Properties to set in table metadata.
 * @param comment User-specified comment that can be placed on the table.
 * @param isStreamingTable if the table is a streaming table, as defined by the source code.
 */
case class Table(
    identifier: TableIdentifier,
    specifiedSchema: Option[StructType],
    partitionCols: Option[Seq[String]],
    clusterCols: Option[Seq[String]],
    normalizedPath: Option[String],
    properties: Map[String, String] = Map.empty,
    comment: Option[String],
    override val origin: QueryOrigin,
    isStreamingTable: Boolean,
    format: Option[String]
) extends TableElement
    with Dataset {

  /** Returns the normalized storage location to this [[Table]]. */
  override def path: String = {
    if (!normalized) {
      throw GraphErrors.unresolvedTablePath(identifier)
    }
    normalizedPath.get
  }

  /**
   * Get the DatasetType of the table
   */
  def datasetType: DatasetType = {
    if (isStreamingTable) {
      DatasetType.STREAMING_TABLE
    } else {
      DatasetType.MATERIALIZED_VIEW
    }
  }
}

/**
 * A virtual table is a representation of a pipeline table used during analysis. During analysis we
 * only care about the schemas of declared tables, and its possible the declared tables do not yet
 * exist in the catalog. Hence we represent all tables in the graph with their "virtual"
 * counterparts, which are simply empty dataframes but with the same schemas.
 *
 * We refer to the declared table that the virtual counterpart represents as the "parent" table
 * below.
 *
 * @param identifier  The identifier of the parent table.
 * @param specifiedSchema The user-specified schema for the parent table.
 * @param incomingFlowIdentifiers The identifiers of all flows that write to the parent table.
 * @param availableFlows  All resolved flows that write to the parent table.
 * @param isStreamingTable  Whether the parent table is a streaming table or not.
 */
case class VirtualTableInput(
    identifier: TableIdentifier,
    specifiedSchema: Option[StructType],
    incomingFlowIdentifiers: Set[TableIdentifier],
    availableFlows: Seq[ResolvedFlow] = Nil,
    isStreamingTable: Boolean
) extends TableElement with Input
    with Logging {
  override def origin: QueryOrigin = QueryOrigin()

  assert(availableFlows.forall(_.destinationIdentifier == identifier))
  override def load: DataFrame = {
    val deducedSchema = specifiedSchema match {
      // If the user specified a schema, use it directly.
      case Some(ss) => ss
      // Otherwise infer the schema from a combination of the incoming flows and the
      // user-specified schema, if provided.
      case _ =>
        SchemaInferenceUtils.inferSchemaFromFlows(availableFlows, specifiedSchema)
    }

    // Produce either a streaming or batch dataframe, depending on whether this is a virtual
    // representation of a streaming or non-streaming table. Return the [empty] dataframe with the
    // deduced schema.
    if (isStreamingTable) {
      MemoryStream[Row](ExpressionEncoder(deducedSchema, lenient = false), spark)
        .toDF()
    } else {
      spark.createDataFrame(new util.ArrayList[Row](), deducedSchema)
    }
  }
}

/**
 * Representing a view in the [[DataflowGraph]].
 */
trait View extends GraphElement {

  /** Returns the unique identifier for this [[View]]. */
  val identifier: TableIdentifier

  /** Properties of this view */
  val properties: Map[String, String]

  /** (SQL-specific) The raw query that defines the [[View]]. */
  val sqlText: Option[String]

  /** User-specified comment that can be placed on the [[View]]. */
  val comment: Option[String]
}

/**
 * Representing a temporary [[View]] in a [[DataflowGraph]].
 */
case class TemporaryView(
    identifier: TableIdentifier,
    properties: Map[String, String],
    sqlText: Option[String],
    comment: Option[String],
    origin: QueryOrigin
) extends View {}

/**
 * Representing a persisted [[View]] in a [[DataflowGraph]].
 */
case class PersistedView(
    identifier: TableIdentifier,
    properties: Map[String, String],
    sqlText: Option[String],
    comment: Option[String],
    origin: QueryOrigin
) extends View {}

trait Sink extends GraphElement with Output {
  /** format of the sink */
  val format: String

  /** options defined for the sink */
  val options: Map[String, String]
}

case class SinkImpl(
   identifier: TableIdentifier,
   format: String,
   options: Map[String, String],
   origin: QueryOrigin
 ) extends Sink {}
