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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.datasources.DataSource

/**
 * Responsible for properly qualify the identifiers for datasets inside or referenced by the
 * dataflow graph.
 */
object GraphIdentifierManager {

  import IdentifierHelper._

  def parseTableIdentifier(name: String, spark: SparkSession): TableIdentifier = {
    toTableIdentifier(spark.sessionState.sqlParser.parseMultipartIdentifier(name))
  }

  /**
   * Fully qualify (if needed) the user-specified identifier used to reference datasets, and
   * categorizing the dataset we're referencing (i.e. dataset from this pipeline or dataset that is
   * external to this pipeline).
   *
   * Returns whether the input dataset should be read as a dataset and also the qualified
   * identifier.
   *
   * @param rawInputName the user-specified name when referencing datasets.
   */
  def parseAndQualifyInputIdentifier(
      context: FlowAnalysisContext,
      rawInputName: String): DatasetIdentifier = {
    resolveDatasetReadInsideQueryDefinition(context = context, rawInputName = rawInputName)
  }

  /**
   * Resolve dataset reads that happens inside the dataset query definition (i.e., inside
   * the @materialized_view() annotation in Python).
   */
  private def resolveDatasetReadInsideQueryDefinition(
      context: FlowAnalysisContext,
      rawInputName: String
  ): DatasetIdentifier = {
    // After identifier is pre-processed, we first check whether we're referencing a
    // single-part-name dataset (e.g., temp view). If so, don't fully qualified the identifier
    // and directly read from it, because single-part-name datasets always out-mask other
    // fully-qualified-datasets that have the same name. For example, if there's a view named
    // "a" and also a table named "catalog.schema.a" defined in the graph. "SELECT * FROM a"
    // would always read the view "a". To read table "a", user would need to read it using
    // fully/partially qualified name (e.g., "SELECT * FROM catalog.schema.a" or "SELECT * FROM
    // schema.a").
    val inputIdentifier = parseTableIdentifier(rawInputName, context.spark)

    /** Return whether we're referencing a dataset that is part of the pipeline. */
    def isInternalDataset(identifier: TableIdentifier): Boolean = {
      context.allInputs.contains(identifier)
    }

    if (isSinglePartIdentifier(inputIdentifier) &&
      isInternalDataset(inputIdentifier)) {
      // reading a single-part-name dataset defined in the dataflow graph (e.g., a view)
      InternalDatasetIdentifier(identifier = inputIdentifier)
    } else if (isPathIdentifier(context.spark, inputIdentifier)) {
      // path-based reference, always read as external dataset
      ExternalDatasetIdentifier(identifier = inputIdentifier)
    } else {
      val fullyQualifiedInputIdentifier = fullyQualifyIdentifier(
        maybeFullyQualifiedIdentifier = inputIdentifier,
        currentCatalog = context.queryContext.currentCatalog,
        currentDatabase = context.queryContext.currentDatabase
      )
      assertIsFullyQualifiedForRead(identifier = fullyQualifiedInputIdentifier)

      if (isInternalDataset(fullyQualifiedInputIdentifier)) {
        InternalDatasetIdentifier(identifier = fullyQualifiedInputIdentifier)
      } else {
        ExternalDatasetIdentifier(fullyQualifiedInputIdentifier)
      }
    }
  }

  /**
   * @param rawDatasetIdentifier the dataset identifier specified by the user.
   */
  @throws[AnalysisException]
  private def parseAndValidatePipelineDatasetIdentifier(
      rawDatasetIdentifier: TableIdentifier): InternalDatasetIdentifier = {
    InternalDatasetIdentifier(identifier = rawDatasetIdentifier)
  }

  /**
   * Parses the table identifier from the raw table identifier and fully qualifies it.
   *
   * @param rawTableIdentifier the raw table identifier
   * @return the parsed table identifier
   */
  @throws[AnalysisException]
  def parseAndQualifyTableIdentifier(
      rawTableIdentifier: TableIdentifier,
      currentCatalog: Option[String],
      currentDatabase: Option[String]
  ): InternalDatasetIdentifier = {
    val pipelineDatasetIdentifier = parseAndValidatePipelineDatasetIdentifier(
      rawDatasetIdentifier = rawTableIdentifier
    )
    val fullyQualifiedTableIdentifier = fullyQualifyIdentifier(
      maybeFullyQualifiedIdentifier = pipelineDatasetIdentifier.identifier,
      currentCatalog = currentCatalog,
      currentDatabase = currentDatabase
    )
    // assert the identifier is properly fully qualified
    assertIsFullyQualifiedForCreate(fullyQualifiedTableIdentifier)
    InternalDatasetIdentifier(identifier = fullyQualifiedTableIdentifier)
  }

  /**
   * Parses and validates the view identifier from the raw view identifier for temporary views.
   *
   * @param rawViewIdentifier the raw view identifier
   * @return the parsed view identifier
   */
  @throws[AnalysisException]
  def parseAndValidateTemporaryViewIdentifier(
      rawViewIdentifier: TableIdentifier): TableIdentifier = {
    val internalDatasetIdentifier = parseAndValidatePipelineDatasetIdentifier(
      rawDatasetIdentifier = rawViewIdentifier
    )
    // Temporary views are not persisted to the catalog in use, therefore should not be qualified.
    if (!isSinglePartIdentifier(internalDatasetIdentifier.identifier)) {
      throw new AnalysisException(
        "MULTIPART_TEMPORARY_VIEW_NAME_NOT_SUPPORTED",
        Map("viewName" -> rawViewIdentifier.unquotedString)
      )
    }
    internalDatasetIdentifier.identifier
  }

  /**
   * Parses and validates the view identifier from the raw view identifier for persisted views.
   *
   * @param rawViewIdentifier the raw view identifier
   * @param currentCatalog the catalog
   * @param currentDatabase the schema
   * @return the parsed view identifier
   */
  def parseAndValidatePersistedViewIdentifier(
      rawViewIdentifier: TableIdentifier,
      currentCatalog: Option[String],
      currentDatabase: Option[String]): TableIdentifier = {
    val internalDatasetIdentifier = parseAndValidatePipelineDatasetIdentifier(
      rawDatasetIdentifier = rawViewIdentifier
    )
    // Persisted views have fully qualified names
    val fullyQualifiedViewIdentifier = fullyQualifyIdentifier(
      maybeFullyQualifiedIdentifier = internalDatasetIdentifier.identifier,
      currentCatalog = currentCatalog,
      currentDatabase = currentDatabase
    )
    // assert the identifier is properly fully qualified
    assertIsFullyQualifiedForCreate(fullyQualifiedViewIdentifier)
    fullyQualifiedViewIdentifier
  }

  /**
   * Parses the flow identifier from the raw flow identifier and fully qualify it.
   *
   * @param rawFlowIdentifier the raw flow identifier
   * @return the parsed flow identifier
   */
  @throws[AnalysisException]
  def parseAndQualifyFlowIdentifier(
      rawFlowIdentifier: TableIdentifier,
      currentCatalog: Option[String],
      currentDatabase: Option[String]
  ): InternalDatasetIdentifier = {
    val internalDatasetIdentifier = parseAndValidatePipelineDatasetIdentifier(
      rawDatasetIdentifier = rawFlowIdentifier
    )

    val fullyQualifiedFlowIdentifier = fullyQualifyIdentifier(
      maybeFullyQualifiedIdentifier = internalDatasetIdentifier.identifier,
      currentCatalog = currentCatalog,
      currentDatabase = currentDatabase
    )

    // assert the identifier is properly fully qualified
    assertIsFullyQualifiedForCreate(fullyQualifiedFlowIdentifier)
    InternalDatasetIdentifier(identifier = fullyQualifiedFlowIdentifier)
  }

  /** Represents the identifier for a dataset that is defined or referenced in a pipeline. */
  sealed trait DatasetIdentifier

  /** Represents the identifier for a dataset that is defined by the current pipeline. */
  case class InternalDatasetIdentifier private (
      identifier: TableIdentifier
  ) extends DatasetIdentifier

  /** Represents the identifier for a dataset that is external to the current pipeline. */
  case class ExternalDatasetIdentifier(identifier: TableIdentifier) extends DatasetIdentifier
}

object IdentifierHelper {

  /**
   * Returns the quoted string for the name parts.
   *
   * @param nameParts the dataset name parts.
   * @return the quoted string for the name parts.
   */
  def toQuotedString(nameParts: Seq[String]): String = {
    toTableIdentifier(nameParts).quotedString
  }

  /**
   * Returns the table identifier constructed from the name parts.
   *
   * @param nameParts the dataset name parts.
   * @return the table identifier constructed from the name parts.
   */
  @throws[UnsupportedOperationException]
  def toTableIdentifier(nameParts: Seq[String]): TableIdentifier = {
    nameParts.length match {
      case 1 => TableIdentifier(tableName = nameParts.head)
      case 2 => TableIdentifier(table = nameParts(1), database = Option(nameParts.head))
      case 3 =>
        TableIdentifier(
          table = nameParts(2),
          database = Option(nameParts(1)),
          catalog = Option(nameParts.head)
        )
      case _ =>
        throw new UnsupportedOperationException(
          s"4+ part table identifier ${nameParts.mkString(".")} is not supported."
        )
    }
  }

  /**
   * Returns the table identifier constructed from the logical plan.
   *
   * @param table the logical plan.
   * @return the table identifier constructed from the logical plan.
   */
  def toTableIdentifier(table: LogicalPlan): TableIdentifier = {
    val parts = table match {
      case r: ResolvedIdentifier => r.identifier.namespace.toSeq :+ r.identifier.name
      case u: UnresolvedIdentifier => u.nameParts
      case u: UnresolvedRelation => u.multipartIdentifier
      case _ =>
        throw new UnsupportedOperationException(s"Unable to resolve name for $table.")
    }
    toTableIdentifier(parts)
  }

  /** Return whether the input identifier is a single-part identifier.  */
  def isSinglePartIdentifier(identifier: TableIdentifier): Boolean = {
    identifier.database.isEmpty && identifier.catalog.isEmpty
  }

  /**
   * Return true if the identifier should be resolved as a path-based reference
   * (i.e., `datasource`.`path`).
   */
  def isPathIdentifier(spark: SparkSession, identifier: TableIdentifier): Boolean = {
    if (identifier.nameParts.length != 2) {
      return false
    }
    val Seq(datasource, path) = identifier.nameParts
    val sqlConf = spark.sessionState.conf

    def isDatasourceValid = {
      try {
        DataSource.lookupDataSource(datasource, sqlConf)
        true
      } catch {
        case _: ClassNotFoundException => false
      }
    }

    // Whether the provided datasource is valid.
    isDatasourceValid
  }

  /** Fully qualifies provided identifier with provided catalog & schema. */
  def fullyQualifyIdentifier(
      maybeFullyQualifiedIdentifier: TableIdentifier,
      currentCatalog: Option[String],
      currentDatabase: Option[String]
  ): TableIdentifier = {
    maybeFullyQualifiedIdentifier.copy(
      database = maybeFullyQualifiedIdentifier.database.orElse(currentDatabase),
      catalog = maybeFullyQualifiedIdentifier.catalog.orElse(currentCatalog)
    )
  }

  /** Assert whether the identifier is properly fully qualified when creating a dataset. */
  def assertIsFullyQualifiedForCreate(identifier: TableIdentifier): Unit = {
    assert(
      identifier.catalog.isDefined && identifier.database.isDefined,
      s"Dataset identifier $identifier is not properly fully qualified, expect a " +
      s"three-part-name <catalog>.<schema>.<table>"
    )
  }

  /** Assert whether the identifier is properly qualified when reading a dataset in a pipeline. */
  def assertIsFullyQualifiedForRead(identifier: TableIdentifier): Unit = {
    assert(
      identifier.catalog.isDefined && identifier.database.isDefined,
      s"Failed to reference dataset $identifier, expect a " +
      s"three-part-name <catalog>.<schema>.<table>"
    )
  }
}
