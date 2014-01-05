package catalyst
package execution

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, Table, StorageDescriptor, SerDeInfo}
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.mapred.InputFormat

import analysis.Catalog
import expressions._
import plans.logical._
import rules._
import types._

import collection.JavaConversions._

class HiveMetastoreCatalog(hiveConf: HiveConf) extends Catalog {
  val client = new HiveMetaStoreClient(hiveConf)

  def lookupRelation(name: String, alias: Option[String]): BaseRelation = {
    val (databaseName, tableName) = name.split("\\.") match {
      case Array(tableOnly) => ("default", tableOnly)
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
          case Array(tableOnly) => ("default", tableOnly)
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

object HiveMetatoreTypes {
  def toDataType(metastoreType: String): DataType =
    metastoreType match {
      case "string" => StringType
      case "int" => IntegerType
      case "double" => DoubleType
      case "bigint" => LongType
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
    Class.forName(table.getSd.getSerdeInfo.getSerializationLib).asInstanceOf[Class[Deserializer]],
    Class.forName(table.getSd.getInputFormat).asInstanceOf[Class[InputFormat[_,_]]],
    Class.forName(table.getSd.getOutputFormat),
    hiveQlTable.getMetadata
  )

   implicit class SchemaAttribute(f: FieldSchema) {
     def toAttribute = AttributeReference(
       f.getName,
       HiveMetatoreTypes.toDataType(f.getType),
       // Since data can be dumped in randomly with no validation, everything is nullable.
       nullable = true
     )(qualifiers = tableName +: alias.toSeq)
   }

  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  // Must be a stable value since new attributes are born here.
  val output = partitionKeys ++ table.getSd.getCols.map(_.toAttribute)
}
