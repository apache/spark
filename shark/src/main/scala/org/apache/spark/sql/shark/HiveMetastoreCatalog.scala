package org.apache.spark.sql
package shark

import scala.util.parsing.combinator.RegexParsers

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, Table}
import org.apache.hadoop.hive.metastore.api.{StorageDescriptor, SerDeInfo}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.AbstractDeserializer
import org.apache.hadoop.mapred.InputFormat

import catalyst.analysis.Catalog
import catalyst.expressions._
import catalyst.plans.logical
import catalyst.plans.logical._
import catalyst.rules._
import catalyst.types._
import execution._

import scala.collection.JavaConversions._

class HiveMetastoreCatalog(shark: SharkContext) extends Catalog with Logging {
  val client = new HiveMetaStoreClient(shark.hiveconf)

  def lookupRelation(
      db: Option[String],
      tableName: String,
      alias: Option[String]): LogicalPlan = {
    val databaseName = db.getOrElse(shark.sessionState.getCurrentDatabase())
    val table = client.getTable(databaseName, tableName)
    val hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(table)
    val partitions =
      if (hiveQlTable.isPartitioned) {
        // TODO: Is 255 the right number to pick?
        client.listPartitions(databaseName, tableName, 255).toSeq
      } else {
        Nil
      }

    // Since HiveQL is case insensitive for table names we make them all lowercase.
    MetastoreRelation(databaseName.toLowerCase, tableName.toLowerCase, alias)(table, partitions)
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    import HiveMetastoreTypes._

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case InsertIntoCreatedTable(db, tableName, child) =>
        val databaseName = db.getOrElse(SessionState.get.getCurrentDatabase())

        val table = new Table()
        val schema =
          child.output.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), ""))

        table.setDbName(databaseName)
        table.setTableName(tableName)
        val sd = new StorageDescriptor()
        table.setSd(sd)
        sd.setCols(schema)

        // TODO: THESE ARE ALL DEFAULTS, WE NEED TO PARSE / UNDERSTAND the output specs.
        sd.setCompressed(false)
        sd.setParameters(Map[String, String]())
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
        val serDeInfo = new SerDeInfo()
        serDeInfo.setName(tableName)
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        serDeInfo.setParameters(Map[String, String]())
        sd.setSerdeInfo(serDeInfo)
        client.createTable(table)

        InsertIntoTable(
          lookupRelation(Some(databaseName), tableName, None).asInstanceOf[BaseRelation],
          Map.empty,
          child)
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child) =>
        val childOutputDataTypes = child.output.map(_.dataType)
        // Only check attributes, not partitionKeys since they are always strings.
        // TODO: Fully support inserting into partitioned tables.
        val tableOutputDataTypes = table.attributes.map(_.dataType)

        if (childOutputDataTypes == tableOutputDataTypes) {
          p
        } else {
          // Only do the casting when child output data types differ from table output data types.
          val castedChildOutput = child.output.zip(table.output).map {
            case (input, table) if input.dataType != table.dataType =>
              Alias(Cast(input, table.dataType), input.name)()
            case (input, _) => input
          }

          p.copy(child = logical.Project(castedChildOutput, child))
        }
    }
  }
}

object HiveMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "bigint" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    "decimal" ^^^ DecimalType |
    "varchar\\((\\d+)\\)".r ^^^ StringType

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ ArrayType

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField,",") <~ ">" ^^ StructType

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
    case ArrayType(elementType) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ShortType =>"tinyint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DecimalType => "decimal"
  }
}

case class MetastoreRelation(databaseName: String, tableName: String, alias: Option[String])
    (val table: Table, val partitions: Seq[Partition])
  extends BaseRelation {

  def hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(table)

  def hiveQlPartitions = partitions.map { p =>
    new org.apache.hadoop.hive.ql.metadata.Partition(hiveQlTable, p)
  }

  val tableDesc = new TableDesc(
    Class.forName(table.getSd.getSerdeInfo.getSerializationLib)
      .asInstanceOf[Class[AbstractDeserializer]],
    Class.forName(table.getSd.getInputFormat).asInstanceOf[Class[InputFormat[_,_]]],
    Class.forName(table.getSd.getOutputFormat),
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
