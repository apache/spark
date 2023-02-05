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

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants._
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat => oldInputClass, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => newInputClass}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, NewHadoopRDD, RDD, UnionRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A trait for subclasses that handle table scans.
 */
private[hive] sealed trait TableReader {
  def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow]

  def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow]
}


/**
 * Helper class for scanning tables stored in Hadoop - e.g., to read Hive tables that reside in the
 * data warehouse directory.
 */
private[hive]
class HadoopTableReader(
    @transient private val attributes: Seq[Attribute],
    @transient private val partitionKeys: Seq[Attribute],
    @transient private val tableDesc: TableDesc,
    @transient private val sparkSession: SparkSession,
    hadoopConf: Configuration)
  extends TableReader with CastSupport with SQLConfHelper with Logging {

  // Hadoop honors "mapreduce.job.maps" as hint,
  // but will ignore when mapreduce.jobtracker.address is "local".
  // https://hadoop.apache.org/docs/r2.7.6/hadoop-mapreduce-client/hadoop-mapreduce-client-core/
  // mapred-default.xml
  //
  // In order keep consistency with Hive, we will let it be 0 in local mode also.
  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopHiveConfigurations(
    sparkSession.sparkContext.conf, hadoopConf)

  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow] =
    makeRDDForTable(
      hiveTable,
      Utils.classForName[Deserializer](tableDesc.getSerdeClassName),
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
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
      "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf

    val tablePath = hiveTable.getPath
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)

    // logDebug("Table input: %s".format(tablePath))
    val hadoopRDD = createHadoopRDD(localTableDesc, inputPathStr)

    val attrsWithIndex = attributes.zipWithIndex
    val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.getConstructor().newInstance()
      DeserializerLock.synchronized {
        deserializer.initialize(hconf, localTableDesc.getProperties)
      }
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
    }

    deserializedHadoopRDD
  }

  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow] = {
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
      partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    // SPARK-5068:get FileStatus and do the filtering locally when the path is not exists
    def verifyPartitionPath(
        partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]]):
        Map[HivePartition, Class[_ <: Deserializer]] = {
      if (!conf.verifyPartitionPath) {
        partitionToDeserializer
      } else {
        val existPathSet = collection.mutable.Set[String]()
        val pathPatternSet = collection.mutable.Set[String]()
        partitionToDeserializer.filter {
          case (partition, partDeserializer) =>
            def updateExistPathSetByPathPattern(pathPatternStr: String): Unit = {
              val pathPattern = new Path(pathPatternStr)
              val fs = pathPattern.getFileSystem(hadoopConf)
              val matches = fs.globStatus(pathPattern)
              matches.foreach(fileStatus => existPathSet += fileStatus.getPath.toString)
            }
            // convert  /demo/data/year/month/day  to  /demo/data/*/*/*/
            def getPathPatternByPath(parNum: Int, tempPath: Path): String = {
              var path = tempPath
              for (i <- (1 to parNum)) path = path.getParent
              val tails = (1 to parNum).map(_ => "*").mkString("/", "/", "/")
              path.toString + tails
            }

            val partPath = partition.getDataLocation
            val partNum = Utilities.getPartitionDesc(partition).getPartSpec.size()
            val pathPatternStr = getPathPatternByPath(partNum, partPath)
            if (!pathPatternSet.contains(pathPatternStr)) {
              pathPatternSet += pathPatternStr
              updateExistPathSetByPathPattern(pathPatternStr)
            }
            existPathSet.contains(partPath.toString)
        }
      }
    }

    val hivePartitionRDDs = verifyPartitionPath(partitionToDeserializer)
      .map { case (partition, partDeserializer) =>
      val partDesc = Utilities.getPartitionDescFromTableDesc(tableDesc, partition, true)
      val partPath = partition.getDataLocation
      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)
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

      val broadcastedHiveConf = _broadcastedHadoopConf
      val localDeserializer = partDeserializer
      val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

      // Splits all attributes into two groups, partition key attributes and those that are not.
      // Attached indices indicate the position of each attribute in the output schema.
      val (partitionKeyAttrs, nonPartitionKeyAttrs) =
        attributes.zipWithIndex.partition { case (attr, _) =>
          partitionKeys.contains(attr)
        }

      def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = partitionKeys.indexOf(attr)
          row(ordinal) = cast(Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
        }
      }

      // Fill all partition keys to the given MutableRow object
      fillPartitionKeys(partValues, mutableRow)

      val tableProperties = tableDesc.getProperties
      val avroSchemaProperties = Seq(AvroTableProperties.SCHEMA_LITERAL,
        AvroTableProperties.SCHEMA_URL).map(_.getPropName())

      // Create local references so that the outer object isn't serialized.
      val localTableDesc = tableDesc

      createHadoopRDD(partDesc, inputPathStr).mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val deserializer = localDeserializer.getConstructor().newInstance()
        // SPARK-13709: For SerDes like AvroSerDe, some essential information (e.g. Avro schema
        // information) may be defined in table properties. Here we should merge table properties
        // and partition properties before initializing the deserializer. Note that partition
        // properties take a higher priority here except for the Avro table properties
        // to support schema evolution: in that case the properties given at table level will
        // be used (for details please check SPARK-26836).
        // For example, a partition may have a different SerDe as the one defined in table
        // properties.
        val props = new Properties(tableProperties)
        partProps.asScala.filterNot { case (k, _) =>
          avroSchemaProperties.contains(k) && tableProperties.containsKey(k)
        }.foreach {
          case (key, value) => props.setProperty(key, value)
        }
        DeserializerLock.synchronized {
          deserializer.initialize(hconf, props)
        }
        // get the table deserializer
        val tableSerDe = localTableDesc.getDeserializerClass.getConstructor().newInstance()
        DeserializerLock.synchronized {
          tableSerDe.initialize(hconf, tableProperties)
        }

        // fill the non partition key attributes
        HadoopTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
          mutableRow, tableSerDe)
      }
    }.toSeq

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
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
        val fs = path.getFileSystem(hadoopConf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      case None => path.toString
    }
  }

  /**
   * True if the new org.apache.hadoop.mapreduce.InputFormat is implemented (except
   * HiveHBaseTableInputFormat where although the new interface is implemented by base HBase class
   * the table inicialization in the Hive layer only happens via the old interface methods -
   * for more details see SPARK-32380).
   */
  private def compatibleWithNewHadoopRDD(inputClass: Class[_ <: oldInputClass[_, _]]): Boolean =
    classOf[newInputClass[_, _]].isAssignableFrom(inputClass) &&
      !inputClass.getName.equalsIgnoreCase("org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat")

  /**
   * The entry of creating a RDD.
   * [SPARK-26630] Using which HadoopRDD will be decided by the input format of tables.
   * The input format of NewHadoopRDD is from `org.apache.hadoop.mapreduce` package while
   * the input format of HadoopRDD is from `org.apache.hadoop.mapred` package.
   */
  private def createHadoopRDD(localTableDesc: TableDesc, inputPathStr: String): RDD[Writable] = {
    val inputFormatClazz = localTableDesc.getInputFileFormatClass
    if (compatibleWithNewHadoopRDD(inputFormatClazz)) {
      createNewHadoopRDD(localTableDesc, inputPathStr)
    } else {
      createOldHadoopRDD(localTableDesc, inputPathStr)
    }
  }

  private def createHadoopRDD(partitionDesc: PartitionDesc, inputPathStr: String): RDD[Writable] = {
    val inputFormatClazz = partitionDesc.getInputFileFormatClass
    if (compatibleWithNewHadoopRDD(inputFormatClazz)) {
      createNewHadoopRDD(partitionDesc, inputPathStr)
    } else {
      createOldHadoopRDD(partitionDesc, inputPathStr)
    }
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each executor.
   */
  private def createOldHadoopRDD(tableDesc: TableDesc, path: String): RDD[Writable] = {
    val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _
    val inputFormatClass = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[oldInputClass[Writable, Writable]]]
    createOldHadoopRDD(inputFormatClass, initializeJobConfFunc)
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each executor.
   */
  private def createOldHadoopRDD(partitionDesc: PartitionDesc, path: String): RDD[Writable] = {
    val initializeJobConfFunc =
      HadoopTableReader.initializeLocalJobConfFunc(path, partitionDesc.getTableDesc) _
    val inputFormatClass = partitionDesc.getInputFileFormatClass
      .asInstanceOf[Class[oldInputClass[Writable, Writable]]]
    createOldHadoopRDD(inputFormatClass, initializeJobConfFunc)
  }

  private def createOldHadoopRDD(
      inputFormatClass: Class[oldInputClass[Writable, Writable]],
      initializeJobConfFunc: JobConf => Unit): RDD[Writable] = {
    val rdd = new HadoopRDD(
      sparkSession.sparkContext,
      _broadcastedHadoopConf,
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }

  /**
   * Creates a NewHadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each executor.
   */
  private def createNewHadoopRDD(tableDesc: TableDesc, path: String): RDD[Writable] = {
    val newJobConf = new JobConf(hadoopConf)
    HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc)(newJobConf)
    val inputFormatClass = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[newInputClass[Writable, Writable]]]
    createNewHadoopRDD(inputFormatClass, newJobConf)
  }

  private def createNewHadoopRDD(partDesc: PartitionDesc, path: String): RDD[Writable] = {
    val newJobConf = new JobConf(hadoopConf)
    HadoopTableReader.initializeLocalJobConfFunc(path, partDesc.getTableDesc)(newJobConf)
    val inputFormatClass = partDesc.getInputFileFormatClass
      .asInstanceOf[Class[newInputClass[Writable, Writable]]]
    createNewHadoopRDD(inputFormatClass, newJobConf)
  }

  private def createNewHadoopRDD(
      inputFormatClass: Class[newInputClass[Writable, Writable]],
      jobConf: JobConf): RDD[Writable] = {
    val rdd = new NewHadoopRDD(
      sparkSession.sparkContext,
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      jobConf
    )

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }

}

private[hive] object HiveTableUtil {

  // copied from PlanUtils.configureJobPropertiesForStorageHandler(tableDesc)
  // that calls Hive.get() which tries to access metastore, but it's not valid in runtime
  // it would be fixed in next version of hive but till then, we should use this instead
  def configureJobPropertiesForStorageHandler(
      tableDesc: TableDesc, conf: Configuration, input: Boolean): Unit = {
    val property = tableDesc.getProperties.getProperty(META_TABLE_STORAGE)
    val storageHandler =
      org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }
}

/**
 * Object to synchronize on when calling org.apache.hadoop.hive.serde2.Deserializer#initialize.
 *
 * [SPARK-17398] org.apache.hive.hcatalog.data.JsonSerDe#initialize calls the non-thread-safe
 * HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector, the results of which are
 * returned by JsonSerDe#getObjectInspector.
 * To protect against this bug in Hive (HIVE-15773/HIVE-21752), we synchronize on this object
 * when calling initialize on Deserializer instances that could be JsonSerDe instances.
 */
private[hive] object DeserializerLock

private[hive] object HadoopTableReader extends HiveInspectors with Logging {
  /**
   * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
   * instantiate a HadoopRDD.
   */
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf): Unit = {
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

  /**
   * Transform all given raw `Writable`s into `Row`s.
   *
   * @param iterator Iterator of all `Writable`s to be transformed
   * @param rawDeser The `Deserializer` associated with the input `Writable`
   * @param nonPartitionKeyAttrs Attributes that should be filled together with their corresponding
   *                             positions in the output schema
   * @param mutableRow A reusable `MutableRow` that should be filled
   * @param tableDeser Table Deserializer
   * @return An `Iterator[Row]` transformed from `iterator`
   */
  def fillObject(
      iterator: Iterator[Writable],
      rawDeser: Deserializer,
      nonPartitionKeyAttrs: Seq[(Attribute, Int)],
      mutableRow: InternalRow,
      tableDeser: Deserializer): Iterator[InternalRow] = {

    val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        rawDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }

    logDebug(soi.toString)

    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.toArray.unzip

    /**
     * Builds specific unwrappers ahead of time according to object inspector
     * types to avoid pattern matching and branching costs per row.
     */
    val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
      _.getFieldObjectInspector match {
        case oi: BooleanObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
        case oi: ByteObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
        case oi: ShortObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
        case oi: IntObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
        case oi: LongObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
        case oi: FloatObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
        case oi: DoubleObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
        case oi: HiveVarcharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveCharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveDecimalObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
        case oi: TimestampObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(value)))
        case oi: DateObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setInt(ordinal, DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
        case oi: BinaryObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value))
        case oi =>
          val unwrapper = unwrapperFor(oi)
          (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
      }
    }

    val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)

    // Map each tuple to a row object
    iterator.map { value =>
      val raw = converter.convert(rawDeser.deserialize(value))
      var i = 0
      val length = fieldRefs.length
      while (i < length) {
        try {
          val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        } catch {
          case ex: Throwable =>
            logError(s"Exception thrown in field <${fieldRefs(i).getFieldName}>")
            throw ex
        }
      }

      mutableRow: InternalRow
    }
  }
}
