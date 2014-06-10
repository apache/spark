/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Hive}
import org.apache.hadoop.hive.ql.plan.{TableDesc, FileSinkDesc}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.{ColumnProjectionUtils, Serializer}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{BooleanType, DataType}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive._
import org.apache.spark.{TaskContext, SparkException}
import org.apache.spark.util.MutablePair

/* Implicits */
import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 * The Hive table scan operator.  Column and partition pruning are both handled.
 *
 * @param attributes Attributes to be fetched from the Hive table.
 * @param relation The Hive table be be scanned.
 * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
 */
@DeveloperApi
case class HiveTableScan(
    attributes: Seq[Attribute],
    relation: MetastoreRelation,
    partitionPruningPred: Option[Expression])(
    @transient val sc: HiveContext)
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

    BindReferences.bindReference(pred, relation.partitionKeys)
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
        val dataType = relation.partitionKeys(ordinal).dataType
        (_: Any, partitionKeys: Array[String]) => {
          castFromString(partitionKeys(ordinal), dataType)
        }
      } else {
        val ref = objectInspector.getAllStructFieldRefs
          .find(_.getFieldName == a.name)
          .getOrElse(sys.error(s"Can't find attribute $a"))
        val fieldObjectInspector = ref.getFieldObjectInspector

        val unwrapHiveData = fieldObjectInspector match {
          case _: HiveVarcharObjectInspector =>
            (value: Any) => value.asInstanceOf[HiveVarchar].getValue
          case _: HiveDecimalObjectInspector =>
            (value: Any) => BigDecimal(value.asInstanceOf[HiveDecimal].bigDecimalValue())
          case _ =>
            identity[Any] _
        }

        (row: Any, _: Array[String]) => {
          val data = objectInspector.getStructFieldData(row, ref)
          val hiveData = unwrapData(data, fieldObjectInspector)
          if (hiveData != null) unwrapHiveData(hiveData) else null
        }
      }
    }
  }

  private def castFromString(value: String, dataType: DataType) = {
    Cast(Literal(value), dataType).eval(null)
  }

  private def addColumnMetadataToConf(hiveConf: HiveConf) {
    // Specifies IDs and internal names of columns to be scanned.
    val neededColumnIDs = attributes.map(a => relation.output.indexWhere(_.name == a.name): Integer)
    val columnInternalNames = neededColumnIDs.map(HiveConf.getColumnInternalName(_)).mkString(",")

    if (attributes.size == relation.output.size) {
      ColumnProjectionUtils.setFullyReadColumns(hiveConf)
    } else {
      ColumnProjectionUtils.appendReadColumnIDs(hiveConf, neededColumnIDs)
    }

    ColumnProjectionUtils.appendReadColumnNames(hiveConf, attributes.map(_.name))

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        relation.tableDesc.getDeserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, columnInternalNames)
  }

  addColumnMetadataToConf(sc.hiveconf)

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
  private[hive] def prunePartitions(partitions: Seq[HivePartition]) = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = relation.partitionKeys.map(_.dataType)
        val castedValues = for ((value, dataType) <- part.getValues.zip(dataTypes)) yield {
          castFromString(value, dataType)
        }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = new GenericRow(castedValues.toArray)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  def execute() = {
    inputRdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator.empty
      } else {
        val mutableRow = new GenericMutableRow(attributes.length)
        val mutablePair = new MutablePair[Any, Array[String]]()
        val buffered = iterator.buffered

        // NOTE (lian): Critical path of Hive table scan, unnecessary FP style code and pattern
        // matching are avoided intentionally.
        val rowsAndPartitionKeys = buffered.head match {
          // With partition keys
          case _: Array[Any] =>
            buffered.map { case array: Array[Any] =>
              val deserializedRow = array(0)
              val partitionKeys = array(1).asInstanceOf[Array[String]]
              mutablePair.update(deserializedRow, partitionKeys)
            }

          // Without partition keys
          case _ =>
            val emptyPartitionKeys = Array.empty[String]
            buffered.map { deserializedRow =>
              mutablePair.update(deserializedRow, emptyPartitionKeys)
            }
        }

        rowsAndPartitionKeys.map { pair =>
          var i = 0
          while (i < attributes.length) {
            mutableRow(i) = attributeFunctions(i)(pair._1, pair._2)
            i += 1
          }
          mutableRow: Row
        }
      }
    }
  }

  def output = attributes
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: SparkPlan,
    overwrite: Boolean)
    (@transient sc: HiveContext)
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
    case (s: String, oi: JavaHiveVarcharObjectInspector) =>
      new HiveVarchar(s, s.size)

    case (bd: BigDecimal, oi: JavaHiveDecimalObjectInspector) =>
      new HiveDecimal(bd.underlying())

    case (row: Row, oi: StandardStructObjectInspector) =>
      val struct = oi.create()
      row.zip(oi.getAllStructFieldRefs: Seq[StructField]).foreach {
        case (data, field) =>
          oi.setStructFieldData(struct, field, wrap(data, field.getFieldObjectInspector))
      }
      struct

    case (s: Seq[_], oi: ListObjectInspector) =>
      val wrappedSeq = s.map(wrap(_, oi.getListElementObjectInspector))
      seqAsJavaList(wrappedSeq)

    case (obj, _) =>
      obj
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
      SparkHiveHadoopWriter.createPathFromString(fileSinkConf.getDirName, conf))

    logger.debug("Saving as hadoop file of type " + valueClass.getSimpleName)

    val writer = new SparkHiveHadoopWriter(conf, fileSinkConf)
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
    val jobConf = new JobConf(sc.hiveconf)
    saveAsHiveFile(
      rdd,
      outputClass,
      fileSinkConf,
      jobConf,
      sc.hiveconf.getBoolean("hive.exec.compress.output", false))

    // TODO: Handle dynamic partitioning.
    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    // Have to construct the format of dbname.tablename.
    val qualifiedTableName = s"${table.databaseName}.${table.tableName}"
    // TODO: Correctly set holdDDLTime.
    // In most of the time, we should have holdDDLTime = false.
    // holdDDLTime will be true when TOK_HOLD_DDLTIME presents in the query as a hint.
    val holdDDLTime = false
    if (partition.nonEmpty) {
      val partitionSpec = partition.map {
        case (key, Some(value)) => key -> value
        case (key, None) => key -> "" // Should not reach here right now.
      }
      val partVals = MetaStoreUtils.getPvals(table.hiveQlTable.getPartCols, partitionSpec)
      db.validatePartitionNameCharacters(partVals)
      // inheritTableSpecs is set to true. It should be set to false for a IMPORT query
      // which is currently considered as a Hive native command.
      val inheritTableSpecs = true
      // TODO: Correctly set isSkewedStoreAsSubdir.
      val isSkewedStoreAsSubdir = false
      db.loadPartition(
        outputPath,
        qualifiedTableName,
        partitionSpec,
        overwrite,
        holdDDLTime,
        inheritTableSpecs,
        isSkewedStoreAsSubdir)
    } else {
      db.loadTable(
        outputPath,
        qualifiedTableName,
        overwrite,
        holdDDLTime)
    }

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.sparkContext.makeRDD(Nil, 1)
  }
}
