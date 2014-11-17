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

package org.apache.spark.sql.hive

import java.io.IOException
import java.util.{List => JList}

import scala.util.parsing.combinator.RegexParsers

import org.apache.hadoop.util.ReflectionUtils

import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.{Table => TTable, Partition => TPartition}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table, HiveException}
import org.apache.hadoop.hive.ql.plan.CreateTableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeException}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.{Catalog, OverrideCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.util.Utils

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] class HiveMetastoreCatalog(hive: HiveContext) extends Catalog with Logging {
  import org.apache.spark.sql.hive.HiveMetastoreTypes._

  /** Connection to hive metastore.  Usages should lock on `this`. */
  protected[hive] val client = Hive.get(hive.hiveconf)

  val caseSensitive: Boolean = false

  def tableExists(db: Option[String], tableName: String): Boolean = {
    val (databaseName, tblName) = processDatabaseAndTableName(
      db.getOrElse(hive.sessionState.getCurrentDatabase), tableName)
    client.getTable(databaseName, tblName, false) != null
  }

  def lookupRelation(
      db: Option[String],
      tableName: String,
      alias: Option[String]): LogicalPlan = synchronized {
    val (databaseName, tblName) =
      processDatabaseAndTableName(db.getOrElse(hive.sessionState.getCurrentDatabase), tableName)
    val table = client.getTable(databaseName, tblName)
    if (table.isView) {
      // if the unresolved relation is from hive view
      // parse the text into logic node.
      HiveQl.createPlanForView(table, alias)
    } else {
      val partitions: Seq[Partition] =
        if (table.isPartitioned) {
          HiveShim.getAllPartitionsOf(client, table).toSeq
        } else {
          Nil
        }

      // Since HiveQL is case insensitive for table names we make them all lowercase.
      MetastoreRelation(
        databaseName, tblName, alias)(
          table.getTTable, partitions.map(part => part.getTPartition))(hive)
    }
  }

  /**
   * Create table with specified database, table name, table description and schema
   * @param databaseName Database Name
   * @param tableName Table Name
   * @param schema Schema of the new table, if not specified, will use the schema
   *               specified in crtTbl
   * @param allowExisting if true, ignore AlreadyExistsException
   * @param desc CreateTableDesc object which contains the SerDe info. Currently
   *               we support most of the features except the bucket.
   */
  def createTable(
      databaseName: String,
      tableName: String,
      schema: Seq[Attribute],
      allowExisting: Boolean = false,
      desc: Option[CreateTableDesc] = None) {
    val hconf = hive.hiveconf

    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val tbl = new Table(dbName, tblName)

    val crtTbl: CreateTableDesc = desc.getOrElse(null)

    // We should respect the passed in schema, unless it's not set
    val hiveSchema: JList[FieldSchema] = if (schema == null || schema.isEmpty) {
      crtTbl.getCols
    } else {
      schema.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), ""))
    }
    tbl.setFields(hiveSchema)

    // Most of code are similar with the DDLTask.createTable() of Hive,
    if (crtTbl != null && crtTbl.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtTbl.getTblProps())
    }

    if (crtTbl != null && crtTbl.getPartCols() != null) {
      tbl.setPartCols(crtTbl.getPartCols())
    }

    if (crtTbl != null && crtTbl.getStorageHandler() != null) {
      tbl.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
        crtTbl.getStorageHandler())
    }

    /*
     * We use LazySimpleSerDe by default.
     *
     * If the user didn't specify a SerDe, and any of the columns are not simple
     * types, we will have to use DynamicSerDe instead.
     */
    if (crtTbl == null || crtTbl.getSerName() == null) {
      val storageHandler = tbl.getStorageHandler()
      if (storageHandler == null) {
        logInfo(s"Default to LazySimpleSerDe for table $dbName.$tblName")
        tbl.setSerializationLib(classOf[LazySimpleSerDe].getName())

        import org.apache.hadoop.mapred.TextInputFormat
        import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
        import org.apache.hadoop.io.Text

        tbl.setInputFormatClass(classOf[TextInputFormat])
        tbl.setOutputFormatClass(classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]])
        tbl.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
      } else {
        val serDeClassName = storageHandler.getSerDeClass().getName()
        logInfo(s"Use StorageHandler-supplied $serDeClassName for table $dbName.$tblName")
        tbl.setSerializationLib(serDeClassName)
      }
    } else {
      // let's validate that the serde exists
      val serdeName = crtTbl.getSerName()
      try {
        val d = ReflectionUtils.newInstance(hconf.getClassByName(serdeName), hconf)
        if (d != null) {
          logDebug("Found class for $serdeName")
        }
      } catch {
        case e: SerDeException => throw new HiveException("Cannot validate serde: " + serdeName, e)
      }
      tbl.setSerializationLib(serdeName)
    }

    if (crtTbl != null && crtTbl.getFieldDelim() != null) {
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, crtTbl.getFieldDelim())
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, crtTbl.getFieldDelim())
    }
    if (crtTbl != null && crtTbl.getFieldEscape() != null) {
      tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR, crtTbl.getFieldEscape())
    }

    if (crtTbl != null && crtTbl.getCollItemDelim() != null) {
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, crtTbl.getCollItemDelim())
    }
    if (crtTbl != null && crtTbl.getMapKeyDelim() != null) {
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, crtTbl.getMapKeyDelim())
    }
    if (crtTbl != null && crtTbl.getLineDelim() != null) {
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, crtTbl.getLineDelim())
    }

    if (crtTbl != null && crtTbl.getSerdeProps() != null) {
      val iter = crtTbl.getSerdeProps().entrySet().iterator()
      while (iter.hasNext()) {
        val m = iter.next()
        tbl.setSerdeParam(m.getKey(), m.getValue())
      }
    }

    if (crtTbl != null && crtTbl.getComment() != null) {
      tbl.setProperty("comment", crtTbl.getComment())
    }

    if (crtTbl != null && crtTbl.getLocation() != null) {
      HiveShim.setLocation(tbl, crtTbl)
    }

    if (crtTbl != null && crtTbl.getSkewedColNames() != null) {
      tbl.setSkewedColNames(crtTbl.getSkewedColNames())
    }
    if (crtTbl != null && crtTbl.getSkewedColValues() != null) {
      tbl.setSkewedColValues(crtTbl.getSkewedColValues())
    }

    if (crtTbl != null) {
      tbl.setStoredAsSubDirectories(crtTbl.isStoredAsSubDirectories())
      tbl.setInputFormatClass(crtTbl.getInputFormat())
      tbl.setOutputFormatClass(crtTbl.getOutputFormat())
    }

    tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName())
    tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName())

    if (crtTbl != null && crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    }

    // set owner
    try {
      tbl.setOwner(hive.hiveconf.getUser)
    } catch {
      case e: IOException => throw new HiveException("Unable to get current user", e)
    }

    // set create time
    tbl.setCreateTime((System.currentTimeMillis() / 1000).asInstanceOf[Int])

    // TODO add bucket support
    // TODO set more info if Hive upgrade

    // create the table
    synchronized {
      try client.createTable(tbl, allowExisting) catch {
        case e: org.apache.hadoop.hive.metastore.api.AlreadyExistsException
          if allowExisting => // Do nothing
        case e: Throwable => throw e
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case CreateTableAsSelect(db, tableName, child, allowExisting, extra) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

        CreateTableAsSelect(Some(databaseName), tableName, child, allowExisting, extra)
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan) = {
      val childOutputDataTypes = child.output.map(_.dataType)
      val tableOutputDataTypes =
        (table.attributes ++ table.partitionKeys).take(child.output.length).map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        p
      } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
        childOutputDataTypes.zip(tableOutputDataTypes)
          .forall { case (left, right) => DataType.equalsIgnoreNullability(left, right) }) {
        // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
        // use InsertIntoHiveTable instead of InsertIntoTable.
        InsertIntoHiveTable(p.table, p.partition, p.child, p.overwrite)
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def registerTable(
      databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit = ???

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def unregisterTable(
      databaseName: Option[String], tableName: String): Unit = ???

  override def unregisterAllTables() = {}
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean)
  extends LogicalPlan {

  override def children = child :: Nil
  override def output = child.output

  override lazy val resolved = childrenResolved && child.output.zip(table.output).forall {
    case (childAttr, tableAttr) =>
      DataType.equalsIgnoreNullability(childAttr.dataType, tableAttr.dataType)
  }
}

/**
 * :: DeveloperApi ::
 * Provides conversions between Spark SQL data types and Hive Metastore types.
 */
@DeveloperApi
object HiveMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ByteType |
    "smallint" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "bigint" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    fixedDecimalType |                     // Hive 0.13+ decimal with precision/scale
    "decimal" ^^^ DecimalType.Unlimited |  // Hive 0.12 decimal with no precision/scale
    "date" ^^^ DateType |
    "timestamp" ^^^ TimestampType |
    "varchar\\((\\d+)\\)".r ^^^ StringType

  protected lazy val fixedDecimalType: Parser[DataType] =
    ("decimal" ~> "(" ~> "\\d+".r) ~ ("," ~> "\\d+".r <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField,",") <~ ">"  ^^ {
      case fields => new StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = parseAll(dataType, metastoreType) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DateType => "date"
    case d: DecimalType => HiveShim.decimalMetastoreString(d)
    case TimestampType => "timestamp"
    case NullType => "void"
    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
  }
}

private[hive] case class MetastoreRelation
    (databaseName: String, tableName: String, alias: Option[String])
    (val table: TTable, val partitions: Seq[TPartition])
    (@transient sqlContext: SQLContext)
  extends LeafNode {

  self: Product =>

  // TODO: Can we use org.apache.hadoop.hive.ql.metadata.Table as the type of table and
  // use org.apache.hadoop.hive.ql.metadata.Partition as the type of elements of partitions.
  // Right now, using org.apache.hadoop.hive.ql.metadata.Table and
  // org.apache.hadoop.hive.ql.metadata.Partition will cause a NotSerializableException
  // which indicates the SerDe we used is not Serializable.

  @transient val hiveQlTable = new Table(table)

  @transient val hiveQlPartitions = partitions.map { p =>
    new Partition(hiveQlTable, p)
  }

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      // TODO: check if this estimate is valid for tables after partition pruning.
      // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
      // relatively cheap if parameters for the table are populated into the metastore.  An
      // alternative would be going through Hadoop's FileSystem API, which can be expensive if a lot
      // of RPCs are involved.  Besides `totalSize`, there are also `numFiles`, `numRows`,
      // `rawDataSize` keys (see StatsSetupConst in Hive) that we can look at in the future.
      BigInt(
        Option(hiveQlTable.getParameters.get(HiveShim.getStatsSetupConstTotalSize))
          .map(_.toLong)
          .getOrElse(sqlContext.defaultSizeInBytes))
    }
  )

  val tableDesc = HiveShim.getTableDesc(
    Class.forName(
      hiveQlTable.getSerializationLib,
      true,
      Utils.getContextOrSparkClassLoader).asInstanceOf[Class[Deserializer]],
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  implicit class SchemaAttribute(f: FieldSchema) {
    def toAttribute = AttributeReference(
      f.getName,
      HiveMetastoreTypes.toDataType(f.getType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = Seq(alias.getOrElse(tableName)))
  }

  // Must be a stable value since new attributes are born here.
  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = hiveQlTable.getCols.map(_.toAttribute)

  val output = attributes ++ partitionKeys

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o,o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex)
}
