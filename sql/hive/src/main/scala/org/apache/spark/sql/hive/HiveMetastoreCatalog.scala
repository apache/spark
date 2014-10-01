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

import scala.util.parsing.combinator.RegexParsers

import org.apache.hadoop.hive.metastore.api.{FieldSchema, StorageDescriptor, SerDeInfo}
import org.apache.hadoop.hive.metastore.api.{Table => TTable, Partition => TPartition}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.stats.StatsSetupConst
import org.apache.hadoop.hive.serde2.Deserializer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.{EliminateAnalysisOperators, Catalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.hive.execution.HiveTableScan
import org.apache.spark.util.Utils

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] class HiveMetastoreCatalog(hive: HiveContext) extends Catalog with Logging {
  import HiveMetastoreTypes._

  /** Connection to hive metastore.  Usages should lock on `this`. */
  protected[hive] val client = Hive.get(hive.hiveconf)

  val caseSensitive: Boolean = false

  def lookupRelation(
      db: Option[String],
      tableName: String,
      alias: Option[String]): LogicalPlan = synchronized {
    val (databaseName, tblName) = processDatabaseAndTableName(
                                    db.getOrElse(hive.sessionState.getCurrentDatabase), tableName)
    val table = client.getTable(databaseName, tblName)
    val partitions: Seq[Partition] =
      if (table.isPartitioned) {
        client.getAllPartitionsForPruner(table).toSeq
      } else {
        Nil
      }

    // Since HiveQL is case insensitive for table names we make them all lowercase.
    MetastoreRelation(
      databaseName, tblName, alias)(
      table.getTTable, partitions.map(part => part.getTPartition))(hive)
  }

  def createTable(
      databaseName: String,
      tableName: String,
      schema: Seq[Attribute],
      allowExisting: Boolean = false): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val table = new Table(dbName, tblName)
    val hiveSchema =
      schema.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), ""))
    table.setFields(hiveSchema)

    val sd = new StorageDescriptor()
    table.getTTable.setSd(sd)
    sd.setCols(hiveSchema)

    // TODO: THESE ARE ALL DEFAULTS, WE NEED TO PARSE / UNDERSTAND the output specs.
    sd.setCompressed(false)
    sd.setParameters(Map[String, String]())
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
    val serDeInfo = new SerDeInfo()
    serDeInfo.setName(tblName)
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    serDeInfo.setParameters(Map[String, String]())
    sd.setSerdeInfo(serDeInfo)

    try client.createTable(table) catch {
      case e: org.apache.hadoop.hive.ql.metadata.HiveException
        if e.getCause.isInstanceOf[org.apache.hadoop.hive.metastore.api.AlreadyExistsException] &&
           allowExisting => // Do nothing.
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

      case CreateTableAsSelect(db, tableName, child) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

        CreateTableAsSelect(Some(databaseName), tableName, child)
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

      case p @ logical.InsertIntoTable(
                   InMemoryRelation(_, _, _,
                     HiveTableScan(_, table, _)), _, child, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan) = {
      val childOutputDataTypes = child.output.map(_.dataType)
      // Only check attributes, not partitionKeys since they are always strings.
      // TODO: Fully support inserting into partitioned tables.
      val tableOutputDataTypes = table.attributes.map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        p
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
    "decimal" ^^^ DecimalType |
    "timestamp" ^^^ TimestampType |
    "varchar\\((\\d+)\\)".r ^^^ StringType

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
    case DecimalType => "decimal"
    case TimestampType => "timestamp"
    case NullType => "void"
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
        Option(hiveQlTable.getParameters.get(StatsSetupConst.TOTAL_SIZE))
          .map(_.toLong)
          .getOrElse(sqlContext.defaultSizeInBytes))
    }
  )

  val tableDesc = new TableDesc(
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
    )(qualifiers = tableName +: alias.toSeq)
  }

  // Must be a stable value since new attributes are born here.
  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = table.getSd.getCols.map(_.toAttribute)

  val output = attributes ++ partitionKeys
}
