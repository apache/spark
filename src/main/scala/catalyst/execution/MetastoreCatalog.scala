package catalyst
package execution

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, Table}
import org.apache.hadoop.hive.metastore.api.{StorageDescriptor, SerDeInfo}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.AbstractDeserializer
import org.apache.hadoop.mapred.InputFormat

import analysis.Catalog
import expressions._
import plans.logical._
import rules._
import types._

import collection.JavaConversions._
import scala.util.parsing.combinator.RegexParsers

class HiveMetastoreCatalog(hiveConf: HiveConf) extends Catalog {
  val client = new HiveMetaStoreClient(hiveConf)

  def lookupRelation(name: String, alias: Option[String]): BaseRelation = {
    val (databaseName, tableName) = name.split("\\.") match {
      case Array(tableOnly) => (SessionState.get.getCurrentDatabase(), tableOnly)
      case Array(db, table) => (db, table)
    }
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
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case InsertIntoCreatedTable(name, child) =>
        val (databaseName, tableName) = name.split("\\.") match {
          case Array(tableOnly) => (SessionState.get.getCurrentDatabase(), tableOnly)
          case Array(db, table) => (db, table)
        }

        val table = new Table()
        val schema = child.output.map(attr => new FieldSchema(attr.name, "string", ""))

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

        InsertIntoTable(lookupRelation(tableName, None), Map.empty, child)
    }
  }
}

object HiveMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "double" ^^^ DoubleType |
    "bigint" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    "(?i)VARCHAR\\((\\d+)\\)".r ^^^ StringType

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

  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  // Must be a stable value since new attributes are born here.
  val output = table.getSd.getCols.map(_.toAttribute) ++ partitionKeys
}
