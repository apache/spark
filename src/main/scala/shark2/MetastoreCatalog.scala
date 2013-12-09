package catalyst
package shark2

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Table
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
    MetastoreRelation(databaseName, tableName, alias)(client.getTable(databaseName, tableName))
  }
}

object HiveMetatoreTypes {
  def toDataType(metastoreType: String): DataType =
    metastoreType match {
      case "string" => StringType
      case "int" => IntegerType
    }
}

case class MetastoreRelation(databaseName: String, tableName: String, alias: Option[String])(val table: Table)
    extends plans.logical.BaseRelation {
  val hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(table)
  val tableDesc = new TableDesc(
    Class.forName(table.getSd.getSerdeInfo.getSerializationLib).asInstanceOf[Class[org.apache.hadoop.hive.serde2.Deserializer]],
    Class.forName(table.getSd.getInputFormat).asInstanceOf[Class[org.apache.hadoop.mapred.InputFormat[_,_]]],
    Class.forName(table.getSd.getOutputFormat),
    hiveQlTable.getSchema
  )

  // Must be a stable value since new attributes are born here.
  val output = table.getSd.getCols.map { col =>
    AttributeReference(
      col.getName,
      HiveMetatoreTypes.toDataType(col.getType),
      true // AHHH, who makes a metastore with no concept of nullalbility?
    )(qualifiers = tableName +: alias.toSeq)
  }
}