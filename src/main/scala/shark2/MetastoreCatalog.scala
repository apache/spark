package catalyst
package shark2

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, Table}
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import analysis.Catalog
import expressions._
import types._

import collection.JavaConversions._
import org.apache.hadoop.hive.ql.plan.TableDesc

class HiveMetastoreCatalog(hiveConf: HiveConf) extends Catalog {
  val client = new HiveMetaStoreClient(hiveConf)

  def lookupRelation(name: String, alias: Option[String]): plans.logical.LogicalPlan = {
    val (databaseName, tableName) = name.split("\\.") match {
      case Array(tableOnly) => ("default", tableOnly)
      case Array(db, table) => (db, table)
    }
    val table = client.getTable(databaseName, tableName)
    val hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(table)
    val partitions =
      if(hiveQlTable.isPartitioned)
        client.listPartitions(databaseName, tableName, 255).toSeq
      else
        Nil

    MetastoreRelation(databaseName, tableName, alias)(table, partitions)
  }
}

object HiveMetatoreTypes {
  def toDataType(metastoreType: String): DataType =
    metastoreType match {
      case "string" => StringType
      case "int" => IntegerType
    }
}

case class MetastoreRelation(databaseName: String, tableName: String, alias: Option[String])(val table: Table, val partitions: Seq[Partition])
    extends plans.logical.BaseRelation {

  def hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(table)
  def hiveQlPartitions = partitions.map(new org.apache.hadoop.hive.ql.metadata.Partition(hiveQlTable, _))
  val tableDesc = new TableDesc(
    Class.forName(table.getSd.getSerdeInfo.getSerializationLib).asInstanceOf[Class[org.apache.hadoop.hive.serde2.Deserializer]],
    Class.forName(table.getSd.getInputFormat).asInstanceOf[Class[org.apache.hadoop.mapred.InputFormat[_,_]]],
    Class.forName(table.getSd.getOutputFormat),
    hiveQlTable.getSchema
  )

   implicit class SchemaAttribute(f: FieldSchema) {
     def toAttribute =
       AttributeReference(
         f.getName,
         HiveMetatoreTypes.toDataType(f.getType),
         true // Since data can be dumped in randomly with no validation, everything is nullable.
       )(qualifiers = tableName +: alias.toSeq)
   }

  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  // Must be a stable value since new attributes are born here.
  val output = partitionKeys ++ table.getSd.getCols.map(_.toAttribute)
}