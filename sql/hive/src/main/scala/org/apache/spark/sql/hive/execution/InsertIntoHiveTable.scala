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

import scala.collection.JavaConversions._

import java.util.{HashMap => JHashMap}

import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}

import org.apache.spark.{SerializableWritable, SparkException, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.{HiveContext, MetastoreRelation, SparkHiveHadoopWriter}
import org.apache.hadoop.hive.conf.HiveConf

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

  @transient lazy val outputClass = newSerializer(table.tableDesc).getSerializedClass
  @transient private lazy val hiveContext = new Context(sc.hiveconf)
  @transient private lazy val db = Hive.get(sc.hiveconf)

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

    case (m: Map[_, _], oi: MapObjectInspector) =>
      val keyOi = oi.getMapKeyObjectInspector
      val valueOi = oi.getMapValueObjectInspector
      val wrappedMap = m.map { case (key, value) => wrap(key, keyOi) -> wrap(value, valueOi) }
      mapAsJavaMap(wrappedMap)

    case (obj, _) =>
      obj
  }

  /**
   * since we should get directory of dynamic partition from upstream RDD
   * reference the code "serializer.serialize(outputData, standardOI) -> dynamicPartPath"
   * So The type of the elment in RDD is (Writable, String)
   */
  def saveAsHiveFile(
      rdd: RDD[(Writable, String)],
      valueClass: Class[_],
      fileSinkConf: FileSinkDesc,
      conf: SerializableWritable[JobConf],
      isCompressed: Boolean,
      dynamicPartNum: Int) {
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    conf.value.setOutputValueClass(valueClass)
    if (fileSinkConf.getTableInfo.getOutputFileFormatClassName == null) {
      throw new SparkException("Output format class not set")
    }
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    conf.value.set("mapred.output.format.class",
      fileSinkConf.getTableInfo.getOutputFileFormatClassName)
    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      conf.value.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(conf.value.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(conf.value.get("mapred.output.compression.type"))
    }
    conf.value.setOutputCommitter(classOf[FileOutputCommitter])

    FileOutputFormat.setOutputPath(
      conf.value,
      SparkHiveHadoopWriter.createPathFromString(fileSinkConf.getDirName, conf.value))
    log.debug("Saving as hadoop file of type " + valueClass.getSimpleName)
    var writer: SparkHiveHadoopWriter = null
    // Map restore writesr for Dynamic Partition
    var writerMap: scala.collection.mutable.HashMap[String, SparkHiveHadoopWriter] = null
    if (dynamicPartNum == 0) {
      writer = new SparkHiveHadoopWriter(conf.value, fileSinkConf)
      writer.preSetup()
      sc.sparkContext.runJob(rdd, writeToFile _)
      writer.commitJob()
    } else {
      writerMap =  new scala.collection.mutable.HashMap[String, SparkHiveHadoopWriter]
      sc.sparkContext.runJob(rdd, writeToFile _)
      for ((k,v) <- writerMap) {
        v.commitJob()
      }
      writerMap.clear()
    }

    def writeToFile(context: TaskContext, iter: Iterator[(Writable, String)]) {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      if (dynamicPartNum == 0) { // for All static partition
        writer.setup(context.stageId, context.partitionId, attemptNumber)
        writer.open()
        // writer for Dynamic Partition
        while(iter.hasNext) {
          val record = iter.next()
          writer.write(record._1)
        }
        writer.close()
        writer.commit()
      } else { // if there is dynamic Partition
        while(iter.hasNext) {
          val record = iter.next()
          val location = fileSinkConf.getDirName
          val partLocation = location + record._2 // different writer related with different file
          def createNewWriter(): SparkHiveHadoopWriter = {
            val tempWriter = new SparkHiveHadoopWriter(conf.value,
              new FileSinkDesc(partLocation, fileSinkConf.getTableInfo, false))
            tempWriter.setup(context.stageId, context.partitionId, attemptNumber)
            tempWriter.open(record._2)
            writerMap += (record._2 -> tempWriter)
            tempWriter
          }
          val writer2 = writerMap.getOrElseUpdate(record._2, createNewWriter)
          writer2.write(record._1)
        }
        for ((k,v) <- writerMap) {
          v.close()
          v.commit()
        }
      }
    }
  }

  /**
   * Returns the Dynamic partition directory for the given row.
   *  @param partCols an array containing the string names of the partition columns
   *
   * we get the last dynamicPartNum elements from partCols and
   * last dynamicPartNum elements from the current row,
   * then we can construct a String for dynamic partition directory
   * For example:
   * for sql: Insert.....tablename(part1,part2) select ....val1,val2 from ...
   *     return: /part1=val1/part2=val2
   * for sql: Insert.....tablename(part1=val1,part2) select ....,val2 from ...
   *     return: /part2=val2
   * for sql: Insert.....tablename(part1=val1,part2,part3) select ....,val2,val3 from ...
   *     return: /part2=val2/part3=val3
   */
  private def getDynamicPartDir(partCols: Array[String],
      row: Row,
      dynamicPartNum: Int,
      defaultPartName: String): String = {
    assert(dynamicPartNum > 0)
    // TODO needs optimization
    partCols
      .takeRight(dynamicPartNum)
      .zip(row.takeRight(dynamicPartNum))
      .map { case (c, v) => s"/$c=${handleNull(v, defaultPartName)}" }
      .mkString
  }

  /**
   * Returns `rowVal` as a String.
   * If `rowVal` is null or equal to "", returns the default partition name.
   */
  private def handleNull(rowVal: Any, defaultPartName: String): String = {
    if (rowVal == null ||String.valueOf(rowVal).length == 0) {
      defaultPartName
    } else {
      String.valueOf(rowVal)
    }
  }

  override def execute() = result

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   *
   * Note: this is run once and then kept to avoid double insertions.
   */
  private lazy val result: RDD[Row] = {
    val childRdd = child.execute()
    assert(childRdd != null)

    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = table.tableDesc
    val tableLocation = table.hiveQlTable.getDataLocation
    val tmpLocation = hiveContext.getExternalTmpFileURI(tableLocation)
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)

    val numDynamicPartitions = partition.values.filter(_.isEmpty).size
    val numStaticPartitions = partition.values.filter(_.isDefined).size
    val partitionSpec = partition.map {
      case (key, Some(value)) =>
        key -> value
      case (key, None) =>
        key -> ""
    }

    val jobConf = new JobConf(sc.hiveconf)
    val jobConfSer = new SerializableWritable(jobConf)
    // check if the partition spec is valid
    if (numDynamicPartitions > 0) {
      if (!sc.hiveconf.getBoolVar(HiveConf.ConfVars.DYNAMICPARTITIONING)) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg())
      }
      if (numStaticPartitions == 0 &&
        sc.hiveconf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg())
      }
      // check if static partition appear after dynamic partitions
      var tmpNumStaticPartitions = numStaticPartitions
      for ((k,v) <- partitionSpec) {
        if (partitionSpec(k) == "") {
          if (tmpNumStaticPartitions > 0) { // found a DP, but there exists ST as subpartition
            throw new SparkException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg())
          }
        } else {
          tmpNumStaticPartitions -= 1
        }
      }
    }

    val rdd = childRdd.mapPartitions { iter =>
      val serializer = newSerializer(fileSinkConf.getTableInfo)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(
          fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
          ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]

      val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      val outputData = new Array[Any](fieldOIs.length)
      val defaultPartName = jobConfSer.value.get(
        "hive.exec.default.partition.name ", "__HIVE_DEFAULT_PARTITION__")

      val partitionColumns = fileSinkConf.getTableInfo.
        getProperties.getProperty("partition_columns") // a String like "colname1/colname2"
      val partitionColumnNames = Option(partitionColumns).map(_.split("/")).orNull

      iter.map { row =>
        var dynamicPartPath: String = null
        if (numDynamicPartitions > 0) {
          dynamicPartPath = getDynamicPartDir(partitionColumnNames, row,
            numDynamicPartitions, defaultPartName)
        }
        var i = 0
        while (i < fieldOIs.length) {
          // Casts Strings to HiveVarchars when necessary.
          outputData(i) = wrap(row(i), fieldOIs(i))
          i += 1
        }
        // pass the dynamicPartPath to downStream RDD
        serializer.serialize(outputData, standardOI) -> dynamicPartPath
      }
    }
    saveAsHiveFile(
      rdd,
      outputClass,
      fileSinkConf,
      jobConfSer,
      sc.hiveconf.getBoolean("hive.exec.compress.output", false),
      numDynamicPartitions)

    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    // Have to construct the format of dbname.tablename.
    val qualifiedTableName = s"${table.databaseName}.${table.tableName}"
    // TODO: Correctly set holdDDLTime.
    // In most of the time, we should have holdDDLTime = false.
    // holdDDLTime will be true when TOK_HOLD_DDLTIME presents in the query as a hint.
    val holdDDLTime = false
    if (partition.nonEmpty) {
      val partVals = MetaStoreUtils.getPvals(table.hiveQlTable.getPartCols, partitionSpec)
      db.validatePartitionNameCharacters(partVals)
      // inheritTableSpecs is set to true. It should be set to false for a IMPORT query
      // which is currently considered as a Hive native command.
      val inheritTableSpecs = true
      // TODO: Correctly set isSkewedStoreAsSubdir.
      val isSkewedStoreAsSubdir = false
      if (numDynamicPartitions > 0) {
        db.loadDynamicPartitions(
          outputPath,
          qualifiedTableName,
          partitionSpec,
          overwrite,
          numDynamicPartitions,
          holdDDLTime,
          isSkewedStoreAsSubdir
        )
      } else {
        db.loadPartition(
          outputPath,
          qualifiedTableName,
          partitionSpec,
          overwrite,
          holdDDLTime,
          inheritTableSpecs,
          isSkewedStoreAsSubdir)
      }
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
