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

import java.util

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.common.`type`.HiveVarchar
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.{Context, ErrorMsg}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaHiveDecimalObjectInspector, JavaHiveVarcharObjectInspector}
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.decimal.Decimal
import org.apache.spark.sql.execution.{Command, SparkPlan, UnaryNode}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.{ ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.{SerializableWritable, SparkException, TaskContext}

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
  extends UnaryNode with Command with HiveInspectors {

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

  def saveAsHiveFile(
      rdd: RDD[Row],
      valueClass: Class[_],
      fileSinkConf: FileSinkDesc,
      conf: SerializableWritable[JobConf],
      writerContainer: SparkHiveWriterContainer): Unit = {
    assert(valueClass != null, "Output value class not set")
    conf.value.setOutputValueClass(valueClass)

    val outputFileFormatClassName = fileSinkConf.getTableInfo.getOutputFileFormatClassName
    assert(outputFileFormatClassName != null, "Output format class not set")
    conf.value.set("mapred.output.format.class", outputFileFormatClassName)
    conf.value.setOutputCommitter(classOf[FileOutputCommitter])

    FileOutputFormat.setOutputPath(
      conf.value,
      SparkHiveWriterContainer.createPathFromString(fileSinkConf.getDirName, conf.value))
    log.debug("Saving as hadoop file of type " + valueClass.getSimpleName)

    writerContainer.driverSideSetup()
    sc.sparkContext.runJob(rdd, writeToFile _)
    writerContainer.commitJob()

    // Note that this function is executed on executor side
    def writeToFile(context: TaskContext, iterator: Iterator[Row]): Unit = {
      val serializer = newSerializer(fileSinkConf.getTableInfo)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(
          fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
          ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]

      val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      val wrappers = fieldOIs.map(wrapperFor)
      val outputData = new Array[Any](fieldOIs.length)

      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      writerContainer.executorSideSetup(context.stageId, context.partitionId, attemptNumber)

      iterator.foreach { row =>
        var i = 0
        while (i < fieldOIs.length) {
          outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row(i))
          i += 1
        }

        writerContainer
          .getLocalFileWriter(row)
          .write(serializer.serialize(outputData, standardOI))
      }

      writerContainer.close()
    }
  }

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   *
   * Note: this is run once and then kept to avoid double insertions.
   */
  override protected[sql] lazy val sideEffectResult: Seq[Row] = {
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = table.tableDesc
    val tableLocation = table.hiveQlTable.getDataLocation
    val tmpLocation = HiveShim.getExternalTmpPath(hiveContext, tableLocation)
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    val isCompressed = sc.hiveconf.getBoolean(
      ConfVars.COMPRESSRESULT.varname, ConfVars.COMPRESSRESULT.defaultBoolVal)

    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      sc.hiveconf.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(sc.hiveconf.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(sc.hiveconf.get("mapred.output.compression.type"))
    }

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).orNull

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!sc.hiveconf.getBoolVar(HiveConf.ConfVars.DYNAMICPARTITIONING)) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        sc.hiveconf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new SparkException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    val jobConf = new JobConf(sc.hiveconf)
    val jobConfSer = new SerializableWritable(jobConf)

    val writerContainer = if (numDynamicPartitions > 0) {
      val dynamicPartColNames = partitionColumnNames.takeRight(numDynamicPartitions)
      new SparkHiveDynamicPartitionWriterContainer(jobConf, fileSinkConf, dynamicPartColNames)
    } else {
      new SparkHiveWriterContainer(jobConf, fileSinkConf)
    }

    saveAsHiveFile(child.execute(), outputClass, fileSinkConf, jobConfSer, writerContainer)

    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    // Have to construct the format of dbname.tablename.
    val qualifiedTableName = s"${table.databaseName}.${table.tableName}"
    // TODO: Correctly set holdDDLTime.
    // In most of the time, we should have holdDDLTime = false.
    // holdDDLTime will be true when TOK_HOLD_DDLTIME presents in the query as a hint.
    val holdDDLTime = false
    if (partition.nonEmpty) {

      // loadPartition call orders directories created on the iteration order of the this map
      val orderedPartitionSpec = new util.LinkedHashMap[String,String]()
      table.hiveQlTable.getPartCols().foreach{
        entry=>
          orderedPartitionSpec.put(entry.getName,partitionSpec.get(entry.getName).getOrElse(""))
      }
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
          orderedPartitionSpec,
          overwrite,
          numDynamicPartitions,
          holdDDLTime,
          isSkewedStoreAsSubdir
        )
      } else {
        db.loadPartition(
          outputPath,
          qualifiedTableName,
          orderedPartitionSpec,
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

    // Invalidate the cache.
    sqlContext.invalidateCache(table)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[Row]
  }
}
