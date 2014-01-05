package catalyst
package execution

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.`lazy`.LazyStruct
import org.apache.hadoop.mapred.JobConf

import expressions.Attribute
import util._

/* Implicits */
import scala.collection.JavaConversions._

case class HiveTableScan(attributes: Seq[Attribute], relation: MetastoreRelation) extends LeafNode {
  @transient
  val hadoopReader = new HadoopTableReader(relation.tableDesc, SharkContext.hiveconf)

  /**
   * The hive object inspector for this table, which can be used to extract values from the
   * serialized row representation.
   */
  @transient
  lazy val objectInspector =
    relation.tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * Functions that extract the requested attributes from the hive output.
   */
  @transient
  protected lazy val attributeFunctions: Seq[(LazyStruct, Array[String]) => AnyRef] = {
    attributes.map { a =>
      if (relation.partitionKeys.contains(a)) {
        val ordinal = relation.partitionKeys.indexOf(a)
        (struct: LazyStruct, partitionKeys: Array[String]) => partitionKeys(ordinal)
      } else {
        val ref = objectInspector.getAllStructFieldRefs
          .find(_.getFieldName == a.name)
          .getOrElse(sys.error(s"Can't find attribute $a"))

        (struct: LazyStruct, _: Array[String]) => {
          val data = objectInspector.getStructFieldData(struct, ref)
          val inspector = ref.getFieldObjectInspector.asInstanceOf[PrimitiveObjectInspector]
          inspector.getPrimitiveJavaObject(data)
        }
      }
    }
  }

  @transient
  def inputRdd = if (!relation.hiveQlTable.isPartitioned) {
    hadoopReader.makeRDDForTable(relation.hiveQlTable)
  } else {
    hadoopReader.makeRDDForPartitionedTable(relation.hiveQlPartitions)
  }

  def execute() = {
    inputRdd.map { row =>
      val values = row match {
        case Array(struct: LazyStruct, partitionKeys: Array[String]) =>
          attributeFunctions.map(_(struct, partitionKeys))
        case struct: LazyStruct =>
          attributeFunctions.map(_(struct, Array.empty))
      }
      buildRow(values.map {
        case "NULL" => null
        case "null" => null
        case varchar: org.apache.hadoop.hive.common.`type`.HiveVarchar => varchar.getValue
        case other => other
      })
    }
  }

  def output = attributes
}

case class InsertIntoHiveTable(
    table: MetastoreRelation, partition: Map[String, String], child: SharkPlan)
    (@transient sc: SharkContext)
  extends UnaryNode {

  /**
   * This file sink / record writer code is only the first step towards implementing this operator
   * correctly and is not actually used yet.
   */
  val desc = new FileSinkDesc("./", table.tableDesc, false)

  val outputClass = {
    val serializer = table.tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, table.tableDesc.getProperties)
    serializer.getSerializedClass
  }

  lazy val conf = new JobConf()

  lazy val writer = HiveFileFormatUtils.getHiveRecordWriter(
    conf,
    table.tableDesc,
    outputClass,
    desc,
    new Path((new org.apache.hadoop.fs.RawLocalFileSystem).getWorkingDirectory, "test.out"),
    null)

  override def otherCopyArgs = sc :: Nil

  def output = child.output

  def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)

    // TODO: write directly to hive
    val tempDir = java.io.File.createTempFile("data", "tsv")
    tempDir.delete()
    tempDir.mkdir()
    childRdd.map(_.map(a => stringOrNull(a.asInstanceOf[AnyRef])).mkString("\001"))
      .saveAsTextFile(tempDir.getCanonicalPath)

    val partitionSpec =
      if (partition.nonEmpty) {
        s"PARTITION (${partition.map { case (k,v) => s"$k=$v" }.mkString(",")})"
      } else {
        ""
      }

    sc.runHive(s"LOAD DATA LOCAL INPATH '${tempDir.getCanonicalPath}/*' INTO TABLE ${table.tableName} $partitionSpec")

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.makeRDD(Nil, 1)
  }
}