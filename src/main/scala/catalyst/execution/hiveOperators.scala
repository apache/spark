package catalyst
package execution

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils
import org.apache.hadoop.hive.ql.plan.{TableDesc, FileSinkDesc}
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._

import expressions.Attribute

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
  protected lazy val attributeFunctions: Seq[(AnyRef, Array[String]) => AnyRef] = {
    attributes.map { a =>
      val ordinal = relation.partitionKeys.indexOf(a)
      if (ordinal >= 0) {
        (_: AnyRef, partitionKeys: Array[String]) => partitionKeys(ordinal)
      } else {
        val ref = objectInspector.getAllStructFieldRefs
          .find(_.getFieldName == a.name)
          .getOrElse(sys.error(s"Can't find attribute $a"))
        (row: AnyRef, _: Array[String]) => {
          val data = objectInspector.getStructFieldData(row, ref)
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
        case Array(deserializedRow: AnyRef, partitionKeys: Array[String]) =>
          attributeFunctions.map(_(deserializedRow, partitionKeys))
        case deserializedRow: AnyRef =>
          attributeFunctions.map(_(deserializedRow, Array.empty))
      }
      buildRow(values.map {
        case n: String if n.toLowerCase == "null" => null
        case varchar: org.apache.hadoop.hive.common.`type`.HiveVarchar => varchar.getValue
        case other => other
      })
    }
  }

  def output = attributes
}

case class InsertIntoHiveTable(
    table: MetastoreRelation, partition: Map[String, Option[String]], child: SharkPlan)
    (@transient sc: SharkContext)
  extends UnaryNode {

  /**
   * This file sink / record writer code is only the first step towards implementing this operator
   * correctly and is not actually used yet.
   */
  val desc = new FileSinkDesc("./", table.tableDesc, false)

  val outputClass = newSerializer(table.tableDesc).getSerializedClass

  lazy val conf = new JobConf()

  lazy val writer = HiveFileFormatUtils.getHiveRecordWriter(
    conf,
    table.tableDesc,
    outputClass,
    desc,
    new Path((new org.apache.hadoop.fs.RawLocalFileSystem).getWorkingDirectory, "test.out"),
    null)

  private def newSerializer(tableDesc: TableDesc) = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[AbstractSerDe]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  override def otherCopyArgs = sc :: Nil

  def output = child.output

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * [[org.apache.hadoop.hive.serde2.SerDe SerDe]] and the [[org.apache.hadoop.mapred.OutputFormat
   * OutputFormat]] provided by the table definition.
   */
  def execute() = {
    require(partition.isEmpty, "Inserting into partitioned table not supported.")
    val childRdd = child.execute()
    assert(childRdd != null)

    // TODO write directly to Hive
    val tempDir = Files.createTempDirectory("data").toFile

    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since AbstractSerDe is not serializable while TableDesc is.
    val tableDesc = table.tableDesc
    childRdd.mapPartitions { iter =>
      val serializer = newSerializer(tableDesc)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(serializer.getObjectInspector, ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]
      iter.map { row =>
        (null, serializer.serialize(Array(row: _*), standardOI))
      }
    }.saveAsHadoopFile(
        tempDir.getCanonicalPath,
        classOf[NullWritable],
        outputClass,
        tableDesc.getOutputFileFormatClass)

    val partitionSpec = if (partition.nonEmpty) {
        partition.map {
          case (k, Some(v)) => s"$k=$v"
          // Dynamic partition inserts
          case (k, None) => s"$k"
        }.mkString(" PARTITION (", ", ", ")")
      } else {
        ""
      }

    val inpath = tempDir.getCanonicalPath + "/*"
    sc.runHive(s"LOAD DATA LOCAL INPATH '$inpath' INTO TABLE ${table.tableName}$partitionSpec")

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.makeRDD(Nil, 1)
  }
}