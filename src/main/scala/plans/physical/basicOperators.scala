package catalyst
package plans
package physical

import expressions._
import plans.logical
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector, StructObjectInspector}
import shark.execution.HadoopTableReader
import shark.{SharkContext, SharkEnv}

import collection.JavaConversions._
import org.apache.hadoop.hive.serde2.`lazy`.LazyPrimitive

case class HiveTableScan(attributes: Seq[Attribute], relation: analysis.MetastoreRelation) extends PhysicalPlan with LeafNode {
  val hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(relation.table)
  val tableDesc = new TableDesc(
    Class.forName(relation.table.getSd.getSerdeInfo.getSerializationLib).asInstanceOf[Class[org.apache.hadoop.hive.serde2.Deserializer]],
    Class.forName(relation.table.getSd.getInputFormat).asInstanceOf[Class[org.apache.hadoop.mapred.InputFormat[_,_]]],
    Class.forName(relation.table.getSd.getOutputFormat),
    hiveQlTable.getSchema
  )

  @transient
  val hadoopReader = new HadoopTableReader(tableDesc, SharkContext.hiveconf)

  /**
    * The hive object inspector for this table, which can be used to extract values from the
    * serialized row representation.
    */
  @transient
  lazy val objectInspector =
    tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * The hive struct field references that correspond to the attributes to be read from this table.
   */
  @transient
  lazy val refs = attributes.map { a =>
    objectInspector.getAllStructFieldRefs
      .find(_.getFieldName == a.name)
      .getOrElse(sys.error(s"Invalid attribute ${a.name} referenced in table $relation"))
  }

  def execute() = {
    hadoopReader.makeRDDForTable(hiveQlTable).map {
      case struct: org.apache.hadoop.hive.serde2.`lazy`.LazyStruct =>
        refs.map { ref =>
          val data = objectInspector.getStructFieldData(struct, ref)
          ref.getFieldObjectInspector.asInstanceOf[PrimitiveObjectInspector].getPrimitiveJavaObject(data)
        }.toSeq
    }
  }

  def output = attributes
}

case class InsertIntoHiveTable(tableName: String, child: PhysicalPlan) extends UnaryNode {
  def output = Seq.empty
}

