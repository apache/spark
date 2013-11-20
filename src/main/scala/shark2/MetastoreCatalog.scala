package catalyst
package shark2

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import analysis.Catalog
import expressions._
import types._

import collection.JavaConversions._

class HiveMetastoreCatalog(hiveConf: HiveConf) extends Catalog {
  protected val client = new HiveMetaStoreClient(hiveConf)

  def lookupRelation(name: String, alias: Option[String]): plans.logical.LogicalPlan = {
    val (databaseName, tableName) = name.split("\\.") match {
      case Array(tableOnly) => ("default", tableOnly)
      case Array(db, table) => (db, table)
    }
    MetastoreRelation(name)(client.getTable(databaseName, tableName))
  }
}

object HiveMetatoreTypes {
  def toDataType(metastoreType: String): DataType =
    metastoreType match {
      case "string" => StringType
      case "int" => IntegerType
    }
}

case class MetastoreRelation(tableName: String)(val table: Table) extends plans.logical.LeafNode {

  // Must be a stable value since new attributes are born here.
  val output = table.getSd.getCols.map { col =>
    AttributeReference(
      col.getName,
      HiveMetatoreTypes.toDataType(col.getType),
      true // AHHH, who makes a metastore with no concept of nullalbility?
    )()
  }
}