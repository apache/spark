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

package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants._
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.{PlanUtils, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}

import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, RDD, UnionRDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DateUtils

/**
 * A trait for subclasses that handle table scans.
 */
private[hive] sealed trait TableReader {
  def makeRDDForTable(hiveTable: HiveTable): RDD[Row]

  def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[Row]
}


/**
 * Helper class for scanning tables stored in Hadoop - e.g., to read Hive tables that reside in the
 * data warehouse directory.
 */
private[hive]
class HadoopTableReader(
    @transient attributes: Seq[Attribute],
    @transient relation: MetastoreRelation,
    @transient sc: HiveContext,
    @transient hiveExtraConf: HiveConf)
  extends TableReader {

  // Hadoop honors "mapred.map.tasks" as hint, but will ignore when mapred.job.tracker is "local".
  // https://hadoop.apache.org/docs/r1.0.4/mapred-default.html
  //
  // In order keep consistency with Hive, we will let it be 0 in local mode also.
  private val _minSplitsPerRDD = if (sc.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(sc.hiveconf.getInt("mapred.map.tasks", 1), sc.sparkContext.defaultMinPartitions)
  }

  // TODO: set aws s3 credentials.

  private val _broadcastedHiveConf =
    sc.sparkContext.broadcast(new SerializableWritable(hiveExtraConf))

  override def makeRDDForTable(hiveTable: HiveTable): RDD[Row] =
    makeRDDForTable(
      hiveTable,
      relation.tableDesc.getDeserializerClass.asInstanceOf[Class[Deserializer]],
      filterOpt = None)

  /**
   * Creates a Hadoop RDD to read data from the target table's data directory. Returns a transformed
   * RDD that contains deserialized rows.
   *
   * @param hiveTable Hive metadata for the table being scanned.
   * @param deserializerClass Class of the SerDe used to deserialize Writables read from Hadoop.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *                  directory being read. If None, then all files are accepted.
   */
  def makeRDDForTable(
      hiveTable: HiveTable,
      deserializerClass: Class[_ <: Deserializer],
      filterOpt: Option[PathFilter]): RDD[Row] = {

    assert(!hiveTable.isPartitioned, """makeRDDForTable() cannot be called on a partitioned table,
      since input formats may differ across partitions. Use makeRDDForTablePartitions() instead.""")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val tableDesc = relation.tableDesc
    val broadcastedHiveConf = _broadcastedHiveConf

    val tablePath = hiveTable.getPath
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)

    // logDebug("Table input: %s".format(tablePath))
    val ifc = hiveTable.getInputFormatClass
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val hadoopRDD = createHadoopRdd(tableDesc, inputPathStr, ifc)

    val attrsWithIndex = attributes.zipWithIndex
    val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))

    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHiveConf.value.value
      val deserializer = deserializerClass.newInstance()
      deserializer.initialize(hconf, tableDesc.getProperties)
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow)
    }

    deserializedHadoopRDD
  }

  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[Row] = {
    val partitionToDeserializer = partitions.map(part =>
      (part, part.getDeserializer.getClass.asInstanceOf[Class[Deserializer]])).toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None)
  }

  /**
   * Create a HadoopRDD for every partition key specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   *
   * @param partitionToDeserializer Mapping from a Hive Partition metadata object to the SerDe
   *     class to use to deserialize input Writables from the corresponding partition.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *     subdirectory of each partition being read. If None, then all files are accepted.
   */
  def makeRDDForPartitionedTable(
      partitionToDeserializer: Map[HivePartition,
      Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter]): RDD[Row] = {
    val hivePartitionRDDs = partitionToDeserializer.map { case (partition, partDeserializer) =>
      val partDesc = Utilities.getPartitionDesc(partition)
      val partPath = HiveShim.getDataLocationPath(partition)
      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)
      val ifc = partDesc.getInputFileFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      // Get partition field info
      val partSpec = partDesc.getPartSpec
      val partProps = partDesc.getProperties

      val partColsDelimited: String = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      // Partitioning columns are delimited by "/"
      val partCols = partColsDelimited.trim().split("/").toSeq
      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      // Create local references so that the outer object isn't serialized.
      val tableDesc = relation.tableDesc
      val broadcastedHiveConf = _broadcastedHiveConf
      val localDeserializer = partDeserializer
      val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))

      // Splits all attributes into two groups, partition key attributes and those that are not.
      // Attached indices indicate the position of each attribute in the output schema.
      val (partitionKeyAttrs, nonPartitionKeyAttrs) =
        attributes.zipWithIndex.partition { case (attr, _) =>
          relation.partitionKeys.contains(attr)
        }

      def fillPartitionKeys(rawPartValues: Array[String], row: MutableRow) = {
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = relation.partitionKeys.indexOf(attr)
          row(ordinal) = Cast(Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
        }
      }

      // Fill all partition keys to the given MutableRow object
      fillPartitionKeys(partValues, mutableRow)

      createHadoopRdd(tableDesc, inputPathStr, ifc).mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val deserializer = localDeserializer.newInstance()
        deserializer.initialize(hconf, partProps)

        // fill the non partition key attributes
        HadoopTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs, mutableRow)
      }
    }.toSeq

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[Row](sc.sparkContext)
    } else {
      new UnionRDD(hivePartitionRDDs(0).context, hivePartitionRDDs)
    }
  }

  /**
   * If `filterOpt` is defined, then it will be used to filter files from `path`. These files are
   * returned in a single, comma-separated string.
   */
  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) =>
        val fs = path.getFileSystem(sc.hiveconf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      case None => path.toString
    }
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each slave.
   */
  private def createHadoopRdd(
    tableDesc: TableDesc,
    path: String,
    inputFormatClass: Class[InputFormat[Writable, Writable]]): RDD[Writable] = {

    val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _

    val rdd = new HadoopRDD(
      sc.sparkContext,
      _broadcastedHiveConf.asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }
}

private[hive] object HadoopTableReader extends HiveInspectors {
  /**
   * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
   * instantiate a HadoopRDD.
   */
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf) {
    FileInputFormat.setInputPaths(jobConf, path)
    if (tableDesc != null) {
      PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

  /**
   * Transform all given raw `Writable`s into `Row`s.
   *
   * @param iterator Iterator of all `Writable`s to be transformed
   * @param deserializer The `Deserializer` associated with the input `Writable`
   * @param nonPartitionKeyAttrs Attributes that should be filled together with their corresponding
   *                             positions in the output schema
   * @param mutableRow A reusable `MutableRow` that should be filled
   * @return An `Iterator[Row]` transformed from `iterator`
   */
  def fillObject(
      iterator: Iterator[Writable],
      deserializer: Deserializer,
      nonPartitionKeyAttrs: Seq[(Attribute, Int)],
      mutableRow: MutableRow): Iterator[Row] = {

    val soi = deserializer.getObjectInspector().asInstanceOf[StructObjectInspector]
    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.unzip

    // Builds specific unwrappers ahead of time according to object inspector types to avoid pattern
    // matching and branching costs per row.
    val unwrappers: Seq[(Any, MutableRow, Int) => Unit] = fieldRefs.map {
      _.getFieldObjectInspector match {
        case oi: BooleanObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
        case oi: ByteObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
        case oi: ShortObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
        case oi: IntObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
        case oi: LongObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
        case oi: FloatObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
        case oi: DoubleObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
        case oi: HiveVarcharObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) =>
            row.setString(ordinal, oi.getPrimitiveJavaObject(value).getValue)
        case oi: HiveDecimalObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) =>
            row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
        case oi: TimestampObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value).clone())
        case oi: DateObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) =>
            row.update(ordinal, DateUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
        case oi: BinaryObjectInspector =>
          (value: Any, row: MutableRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value))
        case oi =>
          (value: Any, row: MutableRow, ordinal: Int) => row(ordinal) = unwrap(value, oi)
      }
    }

    // Map each tuple to a row object
    iterator.map { value =>
      val raw = deserializer.deserialize(value)
      var i = 0
      while (i < fieldRefs.length) {
        val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
        if (fieldValue == null) {
          mutableRow.setNullAt(fieldOrdinals(i))
        } else {
          unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
        }
        i += 1
      }

      mutableRow: Row
    }
  }
}
