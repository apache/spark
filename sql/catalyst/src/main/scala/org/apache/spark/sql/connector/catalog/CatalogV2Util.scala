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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.Collections

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.CurrentUserContext
import org.apache.spark.sql.catalyst.analysis.{AsOfTimestamp, AsOfVersion, NamedRelation, NoSuchDatabaseException, NoSuchFunctionException, NoSuchTableException, TimeTravelSpec}
import org.apache.spark.sql.catalyst.catalog.ClusterBySpec
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{SerdeInfo, TableSpec}
import org.apache.spark.sql.catalyst.util.GeneratedColumn
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, LiteralValue, Transform}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

private[sql] object CatalogV2Util {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /**
   * The list of reserved table properties, which can not be removed or changed directly by
   * the syntax:
   * {{
   *   ALTER TABLE ... SET TBLPROPERTIES ...
   * }}
   *
   * They need specific syntax to modify
   */
  val TABLE_RESERVED_PROPERTIES =
    Seq(TableCatalog.PROP_COMMENT,
      TableCatalog.PROP_LOCATION,
      TableCatalog.PROP_PROVIDER,
      TableCatalog.PROP_OWNER,
      TableCatalog.PROP_EXTERNAL,
      TableCatalog.PROP_IS_MANAGED_LOCATION)

  /**
   * The list of reserved namespace properties, which can not be removed or changed directly by
   * the syntax:
   * {{
   *   ALTER NAMESPACE ... SET PROPERTIES ...
   * }}
   *
   * They need specific syntax to modify
   */
  val NAMESPACE_RESERVED_PROPERTIES =
    Seq(SupportsNamespaces.PROP_COMMENT,
      SupportsNamespaces.PROP_LOCATION,
      SupportsNamespaces.PROP_OWNER)

  /**
   * Apply properties changes to a map and return the result.
   */
  def applyNamespaceChanges(
      properties: Map[String, String],
      changes: Seq[NamespaceChange]): Map[String, String] = {
    applyNamespaceChanges(properties.asJava, changes).asScala.toMap
  }

  /**
   * Apply properties changes to a Java map and return the result.
   */
  def applyNamespaceChanges(
      properties: util.Map[String, String],
      changes: Seq[NamespaceChange]): util.Map[String, String] = {
    val newProperties = new util.HashMap[String, String](properties)

    changes.foreach {
      case set: NamespaceChange.SetProperty =>
        newProperties.put(set.property, set.value)

      case unset: NamespaceChange.RemoveProperty =>
        newProperties.remove(unset.property)

      case _ =>
      // ignore non-property changes
    }

    Collections.unmodifiableMap(newProperties)
  }

  /**
   * Apply properties changes to a map and return the result.
   */
  def applyPropertiesChanges(
      properties: Map[String, String],
      changes: Seq[TableChange]): Map[String, String] = {
    applyPropertiesChanges(properties.asJava, changes).asScala.toMap
  }

  /**
   * Apply properties changes to a Java map and return the result.
   */
  def applyPropertiesChanges(
      properties: util.Map[String, String],
      changes: Seq[TableChange]): util.Map[String, String] = {
    val newProperties = new util.HashMap[String, String](properties)

    changes.foreach {
      case set: SetProperty =>
        newProperties.put(set.property, set.value)

      case unset: RemoveProperty =>
        newProperties.remove(unset.property)

      case _ =>
      // ignore non-property changes
    }

    Collections.unmodifiableMap(newProperties)
  }

  /**
   * Apply ClusterBy changes to a map and return the result.
   */
  def applyClusterByChanges(
      properties: Map[String, String],
      schema: StructType,
      changes: Seq[TableChange]): Map[String, String] = {
    applyClusterByChanges(properties.asJava, schema, changes).asScala.toMap
  }

  /**
   * Apply ClusterBy changes to a Java map and return the result.
   */
  def applyClusterByChanges(
      properties: util.Map[String, String],
      schema: StructType,
      changes: Seq[TableChange]): util.Map[String, String] = {
    val newProperties = new util.HashMap[String, String](properties)

    changes.foreach {
      case clusterBy: ClusterBy =>
        val clusterByProp =
          ClusterBySpec.toProperty(
            schema,
            ClusterBySpec(clusterBy.clusteringColumns.toIndexedSeq),
            conf.resolver)
        newProperties.put(clusterByProp._1, clusterByProp._2)

      case _ =>
      // ignore non-property changes
    }

    Collections.unmodifiableMap(newProperties)
  }

  /**
   * Apply ClusterBy changes to the partitioning transforms and return the result.
   */
  def applyClusterByChanges(
     partitioning: Array[Transform],
     schema: StructType,
     changes: Seq[TableChange]): Array[Transform] = {

    var newPartitioning = partitioning
    // If there is a clusterBy change (only the first one), we overwrite the existing
    // clustering columns.
    val clusterByOpt = changes.collectFirst { case c: ClusterBy => c }
    clusterByOpt.foreach { clusterBy =>
      newPartitioning = partitioning.map {
        case _: ClusterByTransform => ClusterBySpec.extractClusterByTransform(
          schema, ClusterBySpec(clusterBy.clusteringColumns.toIndexedSeq), conf.resolver)
        case other => other
      }
    }

    newPartitioning
  }

  /**
   * Apply schema changes to a schema and return the result.
   */
  def applySchemaChanges(
      schema: StructType,
      changes: Seq[TableChange],
      tableProvider: Option[String],
      statementType: String): StructType = {
    changes.foldLeft(schema) { (schema, change) =>
      change match {
        case add: AddColumn =>
          add.fieldNames match {
            case Array(name) =>
              val field = StructField(name, add.dataType, nullable = add.isNullable)
              val fieldWithDefault: StructField = encodeDefaultValue(add.defaultValue(), field)
              val fieldWithComment: StructField =
                Option(add.comment).map(fieldWithDefault.withComment).getOrElse(fieldWithDefault)
              addField(schema, fieldWithComment, add.position(), tableProvider, statementType, true)
            case names =>
              replace(schema, names.init.toImmutableArraySeq, parent => parent.dataType match {
                case parentType: StructType =>
                  val field = StructField(names.last, add.dataType, nullable = add.isNullable)
                  val fieldWithDefault: StructField = encodeDefaultValue(add.defaultValue(), field)
                  val fieldWithComment: StructField =
                    Option(add.comment).map(fieldWithDefault.withComment)
                      .getOrElse(fieldWithDefault)
                  Some(parent.copy(dataType =
                    addField(parentType, fieldWithComment, add.position(), tableProvider,
                      statementType, true)))
                case _ =>
                  throw new SparkIllegalArgumentException(
                    errorClass = "_LEGACY_ERROR_TEMP_3229",
                    messageParameters = Map("name" -> names.init.last))
              })
          }

        case rename: RenameColumn =>
          replace(schema, rename.fieldNames.toImmutableArraySeq, field =>
            Some(StructField(rename.newName, field.dataType, field.nullable, field.metadata)))

        case update: UpdateColumnType =>
          replace(schema, update.fieldNames.toImmutableArraySeq, field => {
            Some(field.copy(dataType = update.newDataType))
          })

        case update: UpdateColumnNullability =>
          replace(schema, update.fieldNames.toImmutableArraySeq, field => {
            Some(field.copy(nullable = update.nullable))
          })

        case update: UpdateColumnComment =>
          replace(schema, update.fieldNames.toImmutableArraySeq, field =>
            Some(field.withComment(update.newComment)))

        case update: UpdateColumnPosition =>
          def updateFieldPos(struct: StructType, name: String): StructType = {
            val oldField = struct.fields.find(_.name == name).getOrElse {
              throw new SparkIllegalArgumentException(
                errorClass = "_LEGACY_ERROR_TEMP_3230",
                messageParameters = Map("name" -> name))
            }
            val withFieldRemoved = StructType(struct.fields.filter(_ != oldField))
            addField(withFieldRemoved, oldField, update.position(), tableProvider, statementType,
              false)
          }

          update.fieldNames() match {
            case Array(name) =>
              updateFieldPos(schema, name)
            case names =>
              replace(schema, names.init.toImmutableArraySeq, parent => parent.dataType match {
                case parentType: StructType =>
                  Some(parent.copy(dataType = updateFieldPos(parentType, names.last)))
                case _ =>
                  throw new SparkIllegalArgumentException(
                    errorClass = "_LEGACY_ERROR_TEMP_3229",
                    messageParameters = Map("name" -> names.init.last))
              })
          }

        case update: UpdateColumnDefaultValue =>
          replace(schema, update.fieldNames.toImmutableArraySeq, field =>
            // The new DEFAULT value string will be non-empty for any DDL commands that set the
            // default value, such as "ALTER TABLE t ALTER COLUMN c SET DEFAULT ..." (this is
            // enforced by the parser). On the other hand, commands that drop the default value such
            // as "ALTER TABLE t ALTER COLUMN c DROP DEFAULT" will set this string to empty.
            if (update.newDefaultValue().nonEmpty) {
              Some(field.withCurrentDefaultValue(update.newDefaultValue()))
            } else {
              Some(field.clearCurrentDefaultValue())
            })

        case delete: DeleteColumn =>
          replace(schema, delete.fieldNames.toImmutableArraySeq, _ => None, delete.ifExists)

        case _ =>
          // ignore non-schema changes
          schema
      }
    }
  }

  private def addField(
      schema: StructType,
      field: StructField,
      position: ColumnPosition,
      tableProvider: Option[String],
      statementType: String,
      addNewColumnToExistingTable: Boolean): StructType = {
    val newSchema: StructType = if (position == null) {
      schema.add(field)
    } else if (position.isInstanceOf[First]) {
      StructType(field +: schema.fields)
    } else {
      val afterCol = position.asInstanceOf[After].column()
      val fieldIndex = schema.fields.indexWhere(_.name == afterCol)
      if (fieldIndex == -1) {
        throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3228",
          messageParameters = Map("afterCol" -> afterCol))
      }
      val (before, after) = schema.fields.splitAt(fieldIndex + 1)
      StructType(before ++ (field +: after))
    }
    validateTableProviderForDefaultValue(
      newSchema, tableProvider, statementType, addNewColumnToExistingTable)
    constantFoldCurrentDefaultsToExistDefaults(newSchema, statementType)
  }

  private def replace(
      struct: StructType,
      fieldNames: Seq[String],
      update: StructField => Option[StructField],
      ifExists: Boolean = false): StructType = {

    val posOpt = struct.getFieldIndex(fieldNames.head)
    if (posOpt.isEmpty) {
      if (ifExists) {
        // We couldn't find the column to replace, but with IF EXISTS, we will silence the error
        // Currently only DROP COLUMN may pass down the IF EXISTS parameter
        return struct
      } else {
        throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3227",
          messageParameters = Map("fieldName" -> fieldNames.head))
      }
    }

    val pos = posOpt.get
    val field = struct.fields(pos)
    val replacement: Option[StructField] = (fieldNames.tail, field.dataType) match {
      case (Seq(), _) =>
        update(field)

      case (names, struct: StructType) =>
        val updatedType: StructType = replace(struct, names, update, ifExists)
        Some(StructField(field.name, updatedType, field.nullable, field.metadata))

      case (Seq("key"), map @ MapType(keyType, _, _)) =>
        val updated = update(StructField("key", keyType, nullable = false))
            .getOrElse(throw new SparkIllegalArgumentException("_LEGACY_ERROR_TEMP_3226"))
        Some(field.copy(dataType = map.copy(keyType = updated.dataType)))

      case (Seq("key", names @ _*), map @ MapType(keyStruct: StructType, _, _)) =>
        Some(field.copy(dataType = map.copy(keyType = replace(keyStruct, names, update, ifExists))))

      case (Seq("value"), map @ MapType(_, mapValueType, isNullable)) =>
        val updated = update(StructField("value", mapValueType, nullable = isNullable))
            .getOrElse(throw new SparkIllegalArgumentException("_LEGACY_ERROR_TEMP_3225"))
        Some(field.copy(dataType = map.copy(
          valueType = updated.dataType,
          valueContainsNull = updated.nullable)))

      case (Seq("value", names @ _*), map @ MapType(_, valueStruct: StructType, _)) =>
        Some(field.copy(dataType = map.copy(valueType =
          replace(valueStruct, names, update, ifExists))))

      case (Seq("element"), array @ ArrayType(elementType, isNullable)) =>
        val updated = update(StructField("element", elementType, nullable = isNullable))
            .getOrElse(throw new SparkIllegalArgumentException("_LEGACY_ERROR_TEMP_3224"))
        Some(field.copy(dataType = array.copy(
          elementType = updated.dataType,
          containsNull = updated.nullable)))

      case (Seq("element", names @ _*), array @ ArrayType(elementStruct: StructType, _)) =>
        Some(field.copy(dataType = array.copy(elementType =
          replace(elementStruct, names, update, ifExists))))

      case (names, dataType) =>
        if (!ifExists) {
          throw new SparkIllegalArgumentException(
            errorClass = "_LEGACY_ERROR_TEMP_3223",
            messageParameters = Map("name" -> names.head, "dataType" -> dataType.simpleString))
        }
        None
    }

    val newFields = struct.fields.zipWithIndex.flatMap {
      case (_, index) if pos == index =>
        replacement
      case (other, _) =>
        Some(other)
    }

    new StructType(newFields)
  }

  def loadTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      timeTravelSpec: Option[TimeTravelSpec] = None): Option[Table] =
    try {
      Option(getTable(catalog, ident, timeTravelSpec))
    } catch {
      case _: NoSuchTableException => None
      case _: NoSuchDatabaseException => None
    }

  def getTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      timeTravelSpec: Option[TimeTravelSpec] = None): Table = {
    if (timeTravelSpec.nonEmpty) {
      timeTravelSpec.get match {
        case v: AsOfVersion =>
          catalog.asTableCatalog.loadTable(ident, v.version)
        case ts: AsOfTimestamp =>
          catalog.asTableCatalog.loadTable(ident, ts.timestamp)
      }
    } else {
      catalog.asTableCatalog.loadTable(ident)
    }
  }

  def loadFunction(catalog: CatalogPlugin, ident: Identifier): Option[UnboundFunction] = {
    try {
      Option(catalog.asFunctionCatalog.loadFunction(ident))
    } catch {
      case _: NoSuchFunctionException => None
      case _: NoSuchDatabaseException => None
    }
  }

  def loadRelation(catalog: CatalogPlugin, ident: Identifier): Option[NamedRelation] = {
    loadTable(catalog, ident).map(DataSourceV2Relation.create(_, Some(catalog), Some(ident)))
  }

  def supportsV1Command(catalog: CatalogPlugin): Boolean = {
    catalog.name().equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME) &&
      !SQLConf.get.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION).isDefined
  }

  def isSessionCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.name().equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME)
  }

  def convertTableProperties(t: TableSpec): Map[String, String] = {
    val props = convertTableProperties(
      t.properties, t.options, t.serde, t.location, t.comment,
      t.provider, t.external)
    withDefaultOwnership(props)
  }

  private def convertTableProperties(
      properties: Map[String, String],
      options: Map[String, String],
      serdeInfo: Option[SerdeInfo],
      location: Option[String],
      comment: Option[String],
      provider: Option[String],
      external: Boolean = false): Map[String, String] = {
    properties ++
      options ++ // to make the transition to the "option." prefix easier, add both
      options.map { case (key, value) => TableCatalog.OPTION_PREFIX + key -> value } ++
      convertToProperties(serdeInfo) ++
      (if (external) Some(TableCatalog.PROP_EXTERNAL -> "true") else None) ++
      provider.map(TableCatalog.PROP_PROVIDER -> _) ++
      comment.map(TableCatalog.PROP_COMMENT -> _) ++
      location.map(TableCatalog.PROP_LOCATION -> _)
  }

  /**
   * Converts Hive Serde info to table properties. The mapped property keys are:
   *  - INPUTFORMAT/OUTPUTFORMAT: hive.input/output-format
   *  - STORED AS: hive.stored-as
   *  - ROW FORMAT SERDE: hive.serde
   *  - SERDEPROPERTIES: add "option." prefix
   */
  private def convertToProperties(serdeInfo: Option[SerdeInfo]): Map[String, String] = {
    serdeInfo match {
      case Some(s) =>
        s.formatClasses.map { f =>
          Map("hive.input-format" -> f.input, "hive.output-format" -> f.output)
        }.getOrElse(Map.empty) ++
        s.storedAs.map("hive.stored-as" -> _) ++
        s.serde.map("hive.serde" -> _) ++
        s.serdeProperties.map {
          case (key, value) => TableCatalog.OPTION_PREFIX + key -> value
        }
      case None =>
        Map.empty
    }
  }

  def withDefaultOwnership(properties: Map[String, String]): Map[String, String] = {
    properties ++ Map(TableCatalog.PROP_OWNER -> CurrentUserContext.getCurrentUser)
  }

  def getTableProviderCatalog(
      provider: SupportsCatalogOptions,
      catalogManager: CatalogManager,
      options: CaseInsensitiveStringMap): TableCatalog = {
    Option(provider.extractCatalog(options))
      .map(catalogManager.catalog)
      .getOrElse(catalogManager.v2SessionCatalog)
      .asTableCatalog
  }

  /**
   * Converts DS v2 columns to StructType, which encodes column comment and default value to
   * StructField metadata. This is mainly used to define the schema of v2 scan, w.r.t. the columns
   * of the v2 table.
   */
  def v2ColumnsToStructType(columns: Array[Column]): StructType = {
    StructType(columns.map(v2ColumnToStructField))
  }

  private def v2ColumnToStructField(col: Column): StructField = {
    val metadata = Option(col.metadataInJSON()).map(Metadata.fromJson).getOrElse(Metadata.empty)
    var f = StructField(col.name(), col.dataType(), col.nullable(), metadata)
    Option(col.comment()).foreach { comment =>
      f = f.withComment(comment)
    }
    Option(col.defaultValue()).foreach { default =>
      f = encodeDefaultValue(default, f)
    }
    f
  }

  // For built-in file sources, we encode the default value in StructField metadata. An analyzer
  // rule will check the special metadata and change the DML input plan to fill the default value.
  private def encodeDefaultValue(defaultValue: ColumnDefaultValue, f: StructField): StructField = {
    Option(defaultValue).map { default =>
      // The "exist default" is used to back-fill the existing data when new columns are added, and
      // should be a fixed value which was evaluated at the definition time. For example, if the
      // default value is `current_date()`, the "exist default" should be the value of
      // `current_date()` when the column was defined/altered, instead of when back-fall happens.
      // Note: the back-fill here is a logical concept. The data source can keep the existing
      //       data unchanged and let the data reader to return "exist default" for missing
      //       columns.
      val existingDefault = Literal(default.getValue.value(), default.getValue.dataType()).sql
      f.withExistenceDefaultValue(existingDefault).withCurrentDefaultValue(default.getSql)
    }.getOrElse(f)
  }

  /**
   * Converts a StructType to DS v2 columns, which decodes the StructField metadata to v2 column
   * comment and default value or generation expression. This is mainly used to generate DS v2
   * columns from table schema in DDL commands, so that Spark can pass DS v2 columns to DS v2
   * createTable and related APIs.
   */
  def structTypeToV2Columns(schema: StructType): Array[Column] = {
    schema.fields.map(structFieldToV2Column)
  }

  private def structFieldToV2Column(f: StructField): Column = {
    def metadataAsJson(metadata: Metadata): String = {
      if (metadata == Metadata.empty) {
        null
      } else {
        metadata.json
      }
    }
    def metadataWithKeysRemoved(keys: Seq[String]): Metadata = {
      keys.foldLeft(new MetadataBuilder().withMetadata(f.metadata)) {
        (builder, key) => builder.remove(key)
      }.build()
    }

    val isDefaultColumn = f.getCurrentDefaultValue().isDefined &&
      f.getExistenceDefaultValue().isDefined
    val isGeneratedColumn = GeneratedColumn.isGeneratedColumn(f)
    if (isDefaultColumn && isGeneratedColumn) {
      throw new AnalysisException(
        errorClass = "GENERATED_COLUMN_WITH_DEFAULT_VALUE",
        messageParameters = Map(
          "colName" -> f.name,
          "defaultValue" -> f.getCurrentDefaultValue().get,
          "genExpr" -> GeneratedColumn.getGenerationExpression(f).get
        )
      )
    }

    if (isDefaultColumn) {
      val e = analyze(
        f,
        statementType = "Column analysis",
        metadataKey = EXISTS_DEFAULT_COLUMN_METADATA_KEY)

      assert(e.resolved && e.foldable,
        "The existence default value must be a simple SQL string that is resolved and foldable, " +
          "but got: " + f.getExistenceDefaultValue().get)

      val defaultValue = new ColumnDefaultValue(
        f.getCurrentDefaultValue().get, LiteralValue(e.eval(), f.dataType))
      val cleanedMetadata = metadataWithKeysRemoved(
        Seq("comment", CURRENT_DEFAULT_COLUMN_METADATA_KEY, EXISTS_DEFAULT_COLUMN_METADATA_KEY))
      Column.create(f.name, f.dataType, f.nullable, f.getComment().orNull, defaultValue,
        metadataAsJson(cleanedMetadata))
    } else if (isGeneratedColumn) {
      val cleanedMetadata = metadataWithKeysRemoved(
        Seq("comment", GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY))
      Column.create(f.name, f.dataType, f.nullable, f.getComment().orNull,
        GeneratedColumn.getGenerationExpression(f).get, metadataAsJson(cleanedMetadata))
    } else {
      val cleanedMetadata = metadataWithKeysRemoved(Seq("comment"))
      Column.create(f.name, f.dataType, f.nullable, f.getComment().orNull,
        metadataAsJson(cleanedMetadata))
    }
  }
}
