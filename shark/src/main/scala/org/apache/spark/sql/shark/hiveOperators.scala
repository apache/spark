package org.apache.spark.sql
package shark

import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Hive}
import org.apache.hadoop.hive.ql.plan.{TableDesc, FileSinkDesc}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._

import catalyst.expressions._
import catalyst.types.{BooleanType, DataType}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.{TaskContext, SparkException}
import org.apache.hadoop.io.SequenceFile.CompressionType
import scala.Some
import catalyst.expressions.Cast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._

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
  extends LeafNode
  with HiveInspectors {

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
  val hadoopReader = new HadoopTableReader(relation.tableDesc, sc)

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
  private[shark] def prunePartitions(partitions: Seq[HivePartition]) = {
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
        case decimal: org.apache.hadoop.hive.common.`type`.HiveDecimal =>
          BigDecimal(decimal.bigDecimalValue)
        case other => other
      })
    }
  }

  def output = attributes
}

case class InsertIntoHiveTable(
    table: MetastoreRelation, partition: Map[String, Option[String]], child: SparkPlan)
    (@transient sc: SharkContext)
  extends UnaryNode {

  val outputClass = newSerializer(table.tableDesc).getSerializedClass
  @transient private val hiveContext = new Context(sc.hiveconf)
  @transient private val db = Hive.get(sc.hiveconf)

  private def newSerializer(tableDesc: TableDesc): Serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  override def otherCopyArgs = sc :: Nil

  def output = child.output

  /**
   * Wraps with Hive types based on object inspector.
   * TODO: Consolidate all hive OI/data interface code.
   */
  protected def wrap(a: (Any, ObjectInspector)): Any = a match {
    case (s: String, oi: JavaHiveVarcharObjectInspector) => new HiveVarchar(s, s.size)
    case (bd: BigDecimal, oi: JavaHiveDecimalObjectInspector) =>
      new HiveDecimal(bd.underlying())
    case (s: Seq[_], oi: ListObjectInspector) =>
      seqAsJavaList(s.map(wrap(_, oi.getListElementObjectInspector)))
    case (obj, _) => obj
  }

  def saveAsHiveFile(
      rdd: RDD[Writable],
      valueClass: Class[_],
      fileSinkConf: FileSinkDesc,
      conf: JobConf,
      isCompressed: Boolean) {
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    conf.setOutputValueClass(valueClass)
    if (fileSinkConf.getTableInfo.getOutputFileFormatClassName == null) {
      throw new SparkException("Output format class not set")
    }
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    conf.set("mapred.output.format.class", fileSinkConf.getTableInfo.getOutputFileFormatClassName)
    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      conf.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(conf.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(conf.get("mapred.output.compression.type"))
    }
    conf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(
      conf,
      SharkHadoopWriter.createPathFromString(fileSinkConf.getDirName, conf))

    logger.debug("Saving as hadoop file of type " + valueClass.getSimpleName)

    val writer = new SharkHadoopWriter(conf, fileSinkConf)
    writer.preSetup()

    def writeToFile(context: TaskContext, iter: Iterator[Writable]) {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.partitionId, attemptNumber)
      writer.open()

      var count = 0
      while(iter.hasNext) {
        val record = iter.next()
        count += 1
        writer.write(record)
      }

      writer.close()
      writer.commit()
    }

    sc.sparkContext.runJob(rdd, writeToFile _)
    writer.commitJob()
  }

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)

    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = table.tableDesc
    val tableLocation = table.hiveQlTable.getDataLocation
    val tmpLocation = hiveContext.getExternalTmpFileURI(tableLocation)
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    val rdd = childRdd.mapPartitions { iter =>
      val serializer = newSerializer(fileSinkConf.getTableInfo)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(
          fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
          ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]

      iter.map { row =>
        // Casts Strings to HiveVarchars when necessary.
        val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector)
        val mappedRow = row.zip(fieldOIs).map(wrap)

        serializer.serialize(mappedRow.toArray, standardOI)
      }
    }

    // ORC stores compression information in table properties. While, there are other formats
    // (e.g. RCFile) that rely on hadoop configurations to store compression information.
    saveAsHiveFile(
      rdd,
      outputClass,
      fileSinkConf,
      new JobConf(sc.hiveconf),
      sc.hiveconf.getBoolean("hive.exec.compress.output", false))

    // TODO: Correctly set replace and holdDDLTime.
    // TODO: Handle loading into partitioned tables.
    db.loadTable(
      new Path(fileSinkConf.getDirName),
      // Have to construct the format of dbname.tablename.
      s"${table.databaseName}.${table.tableName}",
      false,
      false)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.sparkContext.makeRDD(Nil, 1)
  }
}
