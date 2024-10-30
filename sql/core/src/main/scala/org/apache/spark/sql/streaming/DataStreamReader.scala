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

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{api, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.json.JsonUtils.checkJsonSchema
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.datasources.v2.python.PythonDataSourceV2
import org.apache.spark.sql.execution.datasources.xml.XmlUtils.checkXmlSchema
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
final class DataStreamReader private[sql](sparkSession: SparkSession) extends api.DataStreamReader {
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

  /** @inheritdoc */
  def load(): DataFrame = loadInternal(None)

  private def loadInternal(path: Option[String]): DataFrame = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")
    }

    val optionsWithPath = if (path.isEmpty) {
      extraOptions
    } else {
      extraOptions + ("path" -> path.get)
    }

    val ds = DataSource.lookupDataSource(source, sparkSession.sessionState.conf).
      getConstructor().newInstance()
    // We need to generate the V1 data source so we can pass it to the V2 relation as a shim.
    // We can't be sure at this point whether we'll actually want to use V2, since we don't know the
    // writer or whether the query is continuous.
    val v1DataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = source,
      options = optionsWithPath.originalMap)
    val v1Relation = ds match {
      case _: StreamSourceProvider => Some(StreamingRelation(v1DataSource))
      case _ => None
    }
    ds match {
      // file source v2 does not support streaming yet.
      case provider: TableProvider if !provider.isInstanceOf[FileDataSourceV2] =>
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          source = provider, conf = sparkSession.sessionState.conf)
        val finalOptions = sessionOptions.filter { case (k, _) => !optionsWithPath.contains(k) } ++
            optionsWithPath.originalMap
        val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
        provider match {
          case p: PythonDataSourceV2 => p.setShortName(source)
          case _ =>
        }
        val table = DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
        import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
        table match {
          case _: SupportsRead if table.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
            import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
            Dataset.ofRows(
              sparkSession,
              StreamingRelationV2(
                Some(provider), source, table, dsOptions,
                toAttributes(table.columns.asSchema), None, None, v1Relation))

          // fallback to v1
          // TODO (SPARK-27483): we should move this fallback logic to an analyzer rule.
          case _ => Dataset.ofRows(sparkSession, StreamingRelation(v1DataSource))
        }

      case _ =>
        // Code path for data source v1.
        Dataset.ofRows(sparkSession, StreamingRelation(v1DataSource))
    }
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
    Dataset.ofRows(
      sparkSession,
      UnresolvedRelation(
        identifier,
        new CaseInsensitiveStringMap(extraOptions.toMap.asJava),
        isStreaming = true))
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
}
