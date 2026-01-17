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

package org.apache.spark.sql.classic

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.catalyst.analysis.{NamedStreamingRelation, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedDataSource
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.json.JsonUtils.checkJsonSchema
import org.apache.spark.sql.execution.datasources.xml.XmlUtils.checkXmlSchema
import org.apache.spark.sql.streaming
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Companion object for DataStreamReader with validation utilities.
 */
private[sql] object DataStreamReader {
  /**
   * Pattern for valid source and sink names.
   * Names must only contain ASCII letters, digits, and underscores.
   */
  private val VALID_NAME_PATTERN: Regex = "^[a-zA-Z0-9_]+$".r

  /**
   * Validates that a streaming source name only contains alphanumeric characters and underscores.
   *
   * @param sourceName the source name to validate
   * @throws AnalysisException if the source name contains invalid characters
   */
  def validateSourceName(sourceName: String): Unit = {
    if (!VALID_NAME_PATTERN.pattern.matcher(sourceName).matches()) {
      throw QueryCompilationErrors.invalidStreamingSourceNameError(sourceName)
    }
  }
}

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
final class DataStreamReader private[sql](sparkSession: SparkSession)
  extends streaming.DataStreamReader {
  /** @inheritdoc */
  def format(source: String): this.type = {
    this.source = source
    this
  }

  /** @inheritdoc */
  def schema(schema: StructType): this.type = {
    if (schema != null) {
      val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
      this.userSpecifiedSchema = Option(replaced)
    }
    this
  }

  /** @inheritdoc */
  def option(key: String, value: String): this.type = {
    this.extraOptions += (key -> value)
    this
  }

  /** @inheritdoc */
  def options(options: scala.collection.Map[String, String]): this.type = {
    this.extraOptions ++= options
    this
  }

  /**
   * Specifies a name for the streaming source. This name is used to identify the source
   * in checkpoint metadata and enables stable checkpoint locations for source evolution.
   *
   * @param sourceName the name to assign to this streaming source
   * @since 4.2.0
   */
  @Experimental
  private[sql] def name(sourceName: String): this.type = {
    DataStreamReader.validateSourceName(sourceName)
    this.userProvidedSourceName = Option(sourceName)
    this
  }

  /** @inheritdoc */
  def load(): DataFrame = loadInternal(None)

  private def loadInternal(path: Option[String]): DataFrame = {
    val unresolved = UnresolvedDataSource(
      source,
      userSpecifiedSchema,
      extraOptions,
      isStreaming = true,
      path.toSeq
    )
    val plan = NamedStreamingRelation.withUserProvidedName(unresolved, userProvidedSourceName)
    Dataset.ofRows(sparkSession, plan)
  }

  /** @inheritdoc */
  def load(path: String): DataFrame = {
    if (!sparkSession.sessionState.conf.legacyPathOptionBehavior &&
        extraOptions.contains("path")) {
      throw QueryCompilationErrors.setPathOptionAndCallWithPathParameterError("load")
    }
    loadInternal(Some(path))
  }

  /** @inheritdoc */
  def table(tableName: String): DataFrame = {
    require(tableName != null, "The table name can't be null")
    val identifier = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val unresolved = UnresolvedRelation(
      identifier,
      new CaseInsensitiveStringMap(extraOptions.toMap.asJava),
      isStreaming = true)
    val plan = NamedStreamingRelation.withUserProvidedName(unresolved, userProvidedSourceName)
    Dataset.ofRows(sparkSession, plan)
  }

  override protected def assertNoSpecifiedSchema(operation: String): Unit = {
    if (userSpecifiedSchema.nonEmpty) {
      throw QueryCompilationErrors.userSpecifiedSchemaUnsupportedError(operation)
    }
  }

  override protected def validateJsonSchema(): Unit = userSpecifiedSchema.foreach(checkJsonSchema)

  override protected def validateXmlSchema(): Unit = userSpecifiedSchema.foreach(checkXmlSchema)

  ///////////////////////////////////////////////////////////////////////////////////////
  // Covariant overrides.
  ///////////////////////////////////////////////////////////////////////////////////////

  /** @inheritdoc */
  override def schema(schemaString: String): this.type = super.schema(schemaString)

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type = super.options(options)

  /** @inheritdoc */
  override def json(path: String): DataFrame = super.json(path)

  /** @inheritdoc */
  override def csv(path: String): DataFrame = super.csv(path)

  /** @inheritdoc */
  override def xml(path: String): DataFrame = super.xml(path)

  /** @inheritdoc */
  override def orc(path: String): DataFrame = super.orc(path)

  /** @inheritdoc */
  override def parquet(path: String): DataFrame = super.parquet(path)

  /** @inheritdoc */
  override def text(path: String): DataFrame = super.text(path)

  /** @inheritdoc */
  override def textFile(path: String): Dataset[String] = super.textFile(path)

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  private var userProvidedSourceName: Option[String] = None
}
