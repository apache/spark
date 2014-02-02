package catalyst
package execution

import java.io.{File, IOException}
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.HiveVarchar
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.ql.plan.{TableDesc, FileSinkDesc}
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._

import catalyst.expressions._
import catalyst.types.{BooleanType, DataType}

/* Implicits */
import scala.collection.JavaConversions._

/**
 * The Hive table scan operator.  Column and partition pruning are both handled.
 *
 * @constructor
 * @param attributes Attributes to be fetched from the Hive table.
 * @param relation The Hive table be be scanned.
 * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
 */
case class HiveTableScan(
    attributes: Seq[Attribute],
    relation: MetastoreRelation,
    partitionPruningPred: Option[Expression])(
    @transient val sc: SharkContext)
  extends LeafNode {

  require(partitionPruningPred.isEmpty || relation.hiveQlTable.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private val boundPruningPred = partitionPruningPred.map { pred =>
    require(
      pred.dataType == BooleanType,
      s"Data type of predicate $pred must be BooleanType rather than ${pred.dataType}.")

    BindReferences.bindReference(pred, Seq(relation.partitionKeys))
  }

  @transient
  val hadoopReader = new HadoopTableReader(relation.tableDesc, sc.hiveconf)

  /**
   * The hive object inspector for this table, which can be used to extract values from the
   * serialized row representation.
   */
  @transient
  lazy val objectInspector =
    relation.tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * Functions that extract the requested attributes from the hive output.  Partitioned values are
   * casted from string to its declared data type.
   */
  @transient
  protected lazy val attributeFunctions: Seq[(Any, Array[String]) => Any] = {
    attributes.map { a =>
      val ordinal = relation.partitionKeys.indexOf(a)
      if (ordinal >= 0) {
        (_: Any, partitionKeys: Array[String]) => {
          val value = partitionKeys(ordinal)
          val dataType = relation.partitionKeys(ordinal).dataType
          castFromString(value, dataType)
        }
      } else {
        val ref = objectInspector.getAllStructFieldRefs
          .find(_.getFieldName == a.name)
          .getOrElse(sys.error(s"Can't find attribute $a"))
        (row: Any, _: Array[String]) => {
          val data = objectInspector.getStructFieldData(row, ref)
          unwrapData(data, ref.getFieldObjectInspector)
        }
      }
    }
  }

  def unwrapData(data: Any, oi: ObjectInspector): Any = oi match {
    case pi: PrimitiveObjectInspector => pi.getPrimitiveJavaObject(data)
    case li: ListObjectInspector =>
      Option(
        li.getList(data)).map(_.map(unwrapData(_, li.getListElementObjectInspector)).toSeq).orNull
    case mi: MapObjectInspector =>
      Option(mi.getMap(data)).map(
        _.map {
          case (k,v) =>
            (unwrapData(k, mi.getMapKeyObjectInspector),
             unwrapData(v, mi.getMapValueObjectInspector))
      }.toMap).orNull
    case si: StructObjectInspector =>
      val allRefs = si.getAllStructFieldRefs
      new GenericRow(
        allRefs.map(r => unwrapData(si.getStructFieldData(data,r), r.getFieldObjectInspector)))
  }

  private def castFromString(value: String, dataType: DataType) = {
    Evaluate(Cast(Literal(value), dataType), Nil)
  }

  @transient
  def inputRdd = if (!relation.hiveQlTable.isPartitioned) {
    hadoopReader.makeRDDForTable(relation.hiveQlTable)
  } else {
    hadoopReader.makeRDDForPartitionedTable(prunePartitions(relation.hiveQlPartitions))
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private[catalyst] def prunePartitions(partitions: Seq[HivePartition]) = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = relation.partitionKeys.map(_.dataType)
        val castedValues = for ((value, dataType) <- part.getValues.zip(dataTypes)) yield {
          castFromString(value, dataType)
        }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = new GenericRow(castedValues)
        Evaluate(shouldKeep, Seq(row)).asInstanceOf[Boolean]
      }
    }
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
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)

    // TODO write directly to Hive
    val tempDir = File.createTempFile("catalysthiveout", "")
    tempDir.delete()
    tempDir.mkdir()


    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since AbstractSerDe is not serializable while TableDesc is.
    val tableDesc = table.tableDesc
    childRdd.mapPartitions { iter =>
      val serializer = newSerializer(tableDesc)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(serializer.getObjectInspector, ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]

      iter.map { row =>
        // Casts Strings to HiveVarchars when necessary.
        val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector)
        val mappedRow = row.zip(fieldOIs).map {
          case (s: String, oi: JavaHiveVarcharObjectInspector) => new HiveVarchar(s, s.size)
          case (obj, _) => obj
        }

        (null, serializer.serialize(Array(mappedRow: _*), standardOI))
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
