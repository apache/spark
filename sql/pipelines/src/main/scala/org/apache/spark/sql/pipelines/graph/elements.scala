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

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.common.DatasetType
import org.apache.spark.sql.pipelines.util.{
  BatchReadOptions,
  InputReadOptions,
  SchemaInferenceUtils,
  StreamingReadOptions
}
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
   * @param readOptions Type of input. Used to determine streaming/batch
   * @return Streaming or batch DataFrame of this Input's data.
   */
  def load(readOptions: InputReadOptions): DataFrame
}

/**
 * Represents a node in a [[DataflowGraph]] that can be written to by a [[Flow]].
 * Must be backed by a file source.
 */
sealed trait Output {

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
sealed trait TableInput extends Input {

  /** The user-specified schema for this table. */
  def specifiedSchema: Option[StructType]
}

/**
 * A table representing a materialized dataset in a [[DataflowGraph]].
 *
 * @param identifier The identifier of this table within the graph.
 * @param specifiedSchema The user-specified schema for this table.
 * @param partitionCols What columns the table should be partitioned by when materialized.
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
    normalizedPath: Option[String],
    properties: Map[String, String] = Map.empty,
    comment: Option[String],
    baseOrigin: QueryOrigin,
    isStreamingTable: Boolean,
    format: Option[String]
) extends TableInput
    with Output {

  override val origin: QueryOrigin = baseOrigin.copy(
    objectType = Some("table"),
    objectName = Some(identifier.unquotedString)
  )

  // Load this table's data from underlying storage.
  override def load(readOptions: InputReadOptions): DataFrame = {
    try {
      lazy val tableName = identifier.quotedString

      val df = readOptions match {
        case sro: StreamingReadOptions =>
          spark.readStream.options(sro.userOptions).table(tableName)
        case _: BatchReadOptions =>
          spark.read.table(tableName)
        case _ =>
          throw new IllegalArgumentException("Unhandled `InputReadOptions` type when loading table")
      }

      df
    } catch {
      case NonFatal(e) => throw LoadTableException(displayName, Option(e))
    }
  }

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
 * A type of [[TableInput]] that returns data from a specified schema or from the inferred
 * [[Flow]]s that write to the table.
 */
case class VirtualTableInput(
    identifier: TableIdentifier,
    specifiedSchema: Option[StructType],
    incomingFlowIdentifiers: Set[TableIdentifier],
    availableFlows: Seq[ResolvedFlow] = Nil
) extends TableInput
    with Logging {
  override def origin: QueryOrigin = QueryOrigin()

  assert(availableFlows.forall(_.destinationIdentifier == identifier))
  override def load(readOptions: InputReadOptions): DataFrame = {
    // Infer the schema for this virtual table
    def getFinalSchema: StructType = {
      specifiedSchema match {
        // This is not a backing table, and we have a user-specified schema, so use it directly.
        case Some(ss) => ss
        // Otherwise infer the schema from a combination of the incoming flows and the
        // user-specified schema, if provided.
        case _ =>
          SchemaInferenceUtils.inferSchemaFromFlows(availableFlows, specifiedSchema)
      }
    }

    // create empty streaming/batch df based on input type.
    def createEmptyDF(schema: StructType): DataFrame = readOptions match {
      case _: StreamingReadOptions =>
        MemoryStream[Row](ExpressionEncoder(schema, lenient = false), spark.sqlContext)
          .toDF()
      case _ => spark.createDataFrame(new util.ArrayList[Row](), schema)
    }

    val df = createEmptyDF(getFinalSchema)
    df
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

  /** User-specified comment that can be placed on the [[View]]. */
  val comment: Option[String]
}

/**
 * Representing a temporary [[View]] in a [[DataflowGraph]].
 *
 * @param identifier The identifier of this view within the graph.
 * @param properties Properties of the view
 * @param comment when defining a view
 */
case class TemporaryView(
    identifier: TableIdentifier,
    properties: Map[String, String],
    comment: Option[String],
    origin: QueryOrigin
) extends View {}

/**
 * Representing a persisted [[View]] in a [[DataflowGraph]].
 *
 * @param identifier The identifier of this view within the graph.
 * @param properties Properties of the view
 * @param comment when defining a view
 */
case class PersistedView(
    identifier: TableIdentifier,
    properties: Map[String, String],
    comment: Option[String],
    origin: QueryOrigin
) extends View {}
