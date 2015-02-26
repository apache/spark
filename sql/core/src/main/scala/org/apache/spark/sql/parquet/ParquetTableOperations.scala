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

package org.apache.spark.sql.parquet

import java.io.IOException
import java.lang.{Long => JLong}
import java.text.SimpleDateFormat
import java.text.NumberFormat
import java.util.concurrent.{Callable, TimeUnit}
import java.util.{ArrayList, Collections, Date, List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat => NewFileOutputFormat}
import parquet.hadoop._
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{InitContext, ReadSupport}
import parquet.hadoop.metadata.GlobalMetaData
import parquet.hadoop.util.ContextUtil
import parquet.io.ParquetDecodingException
import parquet.schema.MessageType

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Row, _}
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.{Logging, SerializableWritable, TaskContext}

/**
 * :: DeveloperApi ::
 * Parquet table scan operator. Imports the file that backs the given
 * [[org.apache.spark.sql.parquet.ParquetRelation]] as a ``RDD[Row]``.
 */
private[sql] case class ParquetTableScan(
    attributes: Seq[Attribute],
    relation: ParquetRelation,
    columnPruningPred: Seq[Expression])
  extends LeafNode {

  // The resolution of Parquet attributes is case sensitive, so we resolve the original attributes
  // by exprId. note: output cannot be transient, see
  // https://issues.apache.org/jira/browse/SPARK-1367
  val output = attributes.map(relation.attributeMap)

  // A mapping of ordinals partitionRow -> finalOutput.
  val requestedPartitionOrdinals = {
    val partitionAttributeOrdinals = AttributeMap(relation.partitioningAttributes.zipWithIndex)

    attributes.zipWithIndex.flatMap {
      case (attribute, finalOrdinal) =>
        partitionAttributeOrdinals.get(attribute).map(_ -> finalOrdinal)
    }
  }.toArray

  override def execute(): RDD[Row] = {
    import parquet.filter2.compat.FilterCompat.FilterPredicateCompat

    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])

    val conf: Configuration = ContextUtil.getConfiguration(job)

    relation.path.split(",").foreach { curPath =>
      val qualifiedPath = {
        val path = new Path(curPath)
        path.getFileSystem(conf).makeQualified(path)
      }
      NewFileInputFormat.addInputPath(job, qualifiedPath)
    }

    // Store both requested and original schema in `Configuration`
    conf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(output))
    conf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(relation.output))

    // Store record filtering predicate in `Configuration`
    // Note 1: the input format ignores all predicates that cannot be expressed
    // as simple column predicate filters in Parquet. Here we just record
    // the whole pruning predicate.
    ParquetFilters
      .createRecordFilter(columnPruningPred)
      .map(_.asInstanceOf[FilterPredicateCompat].getFilterPredicate)
      // Set this in configuration of ParquetInputFormat, needed for RowGroupFiltering
      .foreach(ParquetInputFormat.setFilterPredicate(conf, _))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    conf.set(
      SQLConf.PARQUET_CACHE_METADATA,
      sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true"))

    val baseRDD =
      new org.apache.spark.rdd.NewHadoopRDD(
        sc,
        classOf[FilteringParquetRowInputFormat],
        classOf[Void],
        classOf[Row],
        conf)

    if (requestedPartitionOrdinals.nonEmpty) {
      baseRDD.mapPartitionsWithInputSplit { case (split, iter) =>
        val partValue = "([^=]+)=([^=]+)".r
        val partValues =
          split.asInstanceOf[parquet.hadoop.ParquetInputSplit]
            .getPath
            .toString
            .split("/")
            .flatMap {
              case partValue(key, value) => Some(key -> value)
              case _ => None
            }.toMap

        // Convert the partitioning attributes into the correct types
        val partitionRowValues =
          relation.partitioningAttributes
            .map(a => Cast(Literal(partValues(a.name)), a.dataType).eval(EmptyRow))

        new Iterator[Row] {
          def hasNext = iter.hasNext
          def next() = {
            val row = iter.next()._2.asInstanceOf[SpecificMutableRow]

            // Parquet will leave partitioning columns empty, so we fill them in here.
            var i = 0
            while (i < requestedPartitionOrdinals.size) {
              row(requestedPartitionOrdinals(i)._2) =
                partitionRowValues(requestedPartitionOrdinals(i)._1)
              i += 1
            }
            row
          }
        }
      }
    } else {
      baseRDD.map(_._2)
    }
  }

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): ParquetTableScan = {
    val success = validateProjection(prunedAttributes)
    if (success) {
      ParquetTableScan(prunedAttributes, relation, columnPruningPred)
    } else {
      sys.error("Warning: Could not validate Parquet schema projection in pruneColumns")
    }
  }

  /**
   * Evaluates a candidate projection by checking whether the candidate is a subtype
   * of the original type.
   *
   * @param projection The candidate projection.
   * @return True if the projection is valid, false otherwise.
   */
  private def validateProjection(projection: Seq[Attribute]): Boolean = {
    val original: MessageType = relation.parquetSchema
    val candidate: MessageType = ParquetTypesConverter.convertFromAttributes(projection)
    Try(original.checkContains(candidate)).isSuccess
  }
}

/**
 * :: DeveloperApi ::
 * Operator that acts as a sink for queries on RDDs and can be used to
 * store the output inside a directory of Parquet files. This operator
 * is similar to Hive's INSERT INTO TABLE operation in the sense that
 * one can choose to either overwrite or append to a directory. Note
 * that consecutive insertions to the same table must have compatible
 * (source) schemas.
 *
 * WARNING: EXPERIMENTAL! InsertIntoParquetTable with overwrite=false may
 * cause data corruption in the case that multiple users try to append to
 * the same table simultaneously. Inserting into a table that was
 * previously generated by other means (e.g., by creating an HDFS
 * directory and importing Parquet files generated by other tools) may
 * cause unpredicted behaviour and therefore results in a RuntimeException
 * (only detected via filename pattern so will not catch all cases).
 */
@DeveloperApi
private[sql] case class InsertIntoParquetTable(
    relation: ParquetRelation,
    child: SparkPlan,
    overwrite: Boolean = false)
  extends UnaryNode with SparkHadoopMapReduceUtil {

  /**
   * Inserts all rows into the Parquet file.
   */
  override def execute() = {
    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execution will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().

    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)

    val writeSupport =
      if (child.output.map(_.dataType).forall(_.isPrimitive)) {
        log.debug("Initializing MutableRowWriteSupport")
        classOf[org.apache.spark.sql.parquet.MutableRowWriteSupport]
      } else {
        classOf[org.apache.spark.sql.parquet.RowWriteSupport]
      }

    ParquetOutputFormat.setWriteSupportClass(job, writeSupport)

    val conf = ContextUtil.getConfiguration(job)
    RowWriteSupport.setSchema(relation.output, conf)

    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)

    if (overwrite) {
      try {
        fs.delete(fspath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${fspath.toString} prior"
              + s" to InsertIntoParquetTable:\n${e.toString}")
      }
    }
    saveAsHadoopFile(childRdd, relation.path.toString, conf)

    // We return the child RDD to allow chaining (alternatively, one could return nothing).
    childRdd
  }

  override def output = child.output

  /**
   * Stores the given Row RDD as a Hadoop file.
   *
   * Note: We cannot use ``saveAsNewAPIHadoopFile`` from [[org.apache.spark.rdd.PairRDDFunctions]]
   * together with [[org.apache.spark.util.MutablePair]] because ``PairRDDFunctions`` uses
   * ``Tuple2`` and not ``Product2``. Also, we want to allow appending files to an existing
   * directory and need to determine which was the largest written file index before starting to
   * write.
   *
   * @param rdd The [[org.apache.spark.rdd.RDD]] to writer
   * @param path The directory to write to.
   * @param conf A [[org.apache.hadoop.conf.Configuration]].
   */
  private def saveAsHadoopFile(
      rdd: RDD[Row],
      path: String,
      conf: Configuration) {
    val job = new Job(conf)
    val keyType = classOf[Void]
    job.setOutputKeyClass(keyType)
    job.setOutputValueClass(classOf[Row])
    NewFileOutputFormat.setOutputPath(job, new Path(path))
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()

    val taskIdOffset =
      if (overwrite) {
        1
      } else {
        FileSystemHelper
          .findMaxTaskId(NewFileOutputFormat.getOutputPath(job).toString, job.getConfiguration) + 1
      }

    def writeShard(context: TaskContext, iter: Iterator[Row]): Int = {
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        context.attemptNumber)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = new AppendingParquetOutputFormat(taskIdOffset)
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext)
      try {
        while (iter.hasNext) {
          val row = iter.next()
          writer.write(null, row)
        }
      } finally {
        writer.close(hadoopContext)
      }
      committer.commitTask(hadoopContext)
      1
    }
    val jobFormat = new AppendingParquetOutputFormat(taskIdOffset)
    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    sqlContext.sparkContext.runJob(rdd, writeShard _)
    jobCommitter.commitJob(jobTaskContext)
  }
}

/**
 * TODO: this will be able to append to directories it created itself, not necessarily
 * to imported ones.
 */
private[parquet] class AppendingParquetOutputFormat(offset: Int)
  extends parquet.hadoop.ParquetOutputFormat[Row] {
  // override to accept existing directories as valid output directory
  override def checkOutputSpecs(job: JobContext): Unit = {}

  // override to choose output filename so not overwrite existing ones
  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val taskId: TaskID = getTaskAttemptID(context).getTaskID
    val partition: Int = taskId.getId
    val filename = "part-r-" + numfmt.format(partition + offset) + ".parquet"
    val committer: FileOutputCommitter =
      getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
    new Path(committer.getWorkPath, filename)
  }

  // The TaskAttemptContext is a class in hadoop-1 but is an interface in hadoop-2.
  // The signatures of the method TaskAttemptContext.getTaskAttemptID for the both versions
  // are the same, so the method calls are source-compatible but NOT binary-compatible because
  // the opcode of method call for class is INVOKEVIRTUAL and for interface is INVOKEINTERFACE.
  private def getTaskAttemptID(context: TaskAttemptContext): TaskAttemptID = {
    context.getClass.getMethod("getTaskAttemptID").invoke(context).asInstanceOf[TaskAttemptID]
  }
}

/**
 * We extend ParquetInputFormat in order to have more control over which
 * RecordFilter we want to use.
 */
private[parquet] class FilteringParquetRowInputFormat
  extends parquet.hadoop.ParquetInputFormat[Row] with Logging {

  private var fileStatuses = Map.empty[Path, FileStatus]

  override def createRecordReader(
      inputSplit: InputSplit,
      taskAttemptContext: TaskAttemptContext): RecordReader[Void, Row] = {

    import parquet.filter2.compat.FilterCompat.NoOpFilter

    val readSupport: ReadSupport[Row] = new RowReadSupport()

    val filter = ParquetInputFormat.getFilter(ContextUtil.getConfiguration(taskAttemptContext))
    if (!filter.isInstanceOf[NoOpFilter]) {
      new ParquetRecordReader[Row](
        readSupport,
        filter)
    } else {
      new ParquetRecordReader[Row](readSupport)
    }
  }

  // This is only a temporary solution sicne we need to use fileStatuses in
  // both getClientSideSplits and getTaskSideSplits. It can be removed once we get rid of these
  // two methods.
  override def getSplits(jobContext: JobContext): JList[InputSplit] = {
    // First set fileStatuses.
    val statuses = listStatus(jobContext)
    fileStatuses = statuses.map(file => file.getPath -> file).toMap

    super.getSplits(jobContext)
  }

  // TODO Remove this method and related code once PARQUET-16 is fixed
  // This method together with the `getFooters` method and the `fileStatuses` field are just used
  // to mimic this PR: https://github.com/apache/incubator-parquet-mr/pull/17
  override def getSplits(
      configuration: Configuration,
      footers: JList[Footer]): JList[ParquetInputSplit] = {

    // Use task side strategy by default
    val taskSideMetaData = configuration.getBoolean(ParquetInputFormat.TASK_SIDE_METADATA, true)
    val maxSplitSize: JLong = configuration.getLong("mapred.max.split.size", Long.MaxValue)
    val minSplitSize: JLong =
      Math.max(getFormatMinSplitSize, configuration.getLong("mapred.min.split.size", 0L))
    if (maxSplitSize < 0 || minSplitSize < 0) {
      throw new ParquetDecodingException(
        s"maxSplitSize or minSplitSie should not be negative: maxSplitSize = $maxSplitSize;" +
          s" minSplitSize = $minSplitSize")
    }

    // Uses strict type checking by default
    val getGlobalMetaData =
      classOf[ParquetFileWriter].getDeclaredMethod("getGlobalMetaData", classOf[JList[Footer]])
    getGlobalMetaData.setAccessible(true)
    var globalMetaData = getGlobalMetaData.invoke(null, footers).asInstanceOf[GlobalMetaData]

    if (globalMetaData == null) {
     val splits = mutable.ArrayBuffer.empty[ParquetInputSplit]
     return splits
    }

    Option(globalMetaData.getKeyValueMetaData.get(RowReadSupport.SPARK_METADATA_KEY)).foreach {
      schemas =>
        val mergedSchema = schemas
          .map(DataType.fromJson(_).asInstanceOf[StructType])
          .reduce(_ merge _)
          .json

        val mergedMetadata = globalMetaData
          .getKeyValueMetaData
          .updated(RowReadSupport.SPARK_METADATA_KEY, setAsJavaSet(Set(mergedSchema)))

        globalMetaData = new GlobalMetaData(
          globalMetaData.getSchema,
          mergedMetadata,
          globalMetaData.getCreatedBy)
    }

    val readContext = getReadSupport(configuration).init(
      new InitContext(configuration,
        globalMetaData.getKeyValueMetaData,
        globalMetaData.getSchema))

    if (taskSideMetaData){
      logInfo("Using Task Side Metadata Split Strategy")
      getTaskSideSplits(configuration,
        footers,
        maxSplitSize,
        minSplitSize,
        readContext)
    } else {
      logInfo("Using Client Side Metadata Split Strategy")
      getClientSideSplits(configuration,
        footers,
        maxSplitSize,
        minSplitSize,
        readContext)
    }

  }

  def getClientSideSplits(
    configuration: Configuration,
    footers: JList[Footer],
    maxSplitSize: JLong,
    minSplitSize: JLong,
    readContext: ReadContext): JList[ParquetInputSplit] = {

    import parquet.filter2.compat.FilterCompat.Filter
    import parquet.filter2.compat.RowGroupFilter
    import org.apache.spark.sql.parquet.FilteringParquetRowInputFormat.blockLocationCache

    val cacheMetadata = configuration.getBoolean(SQLConf.PARQUET_CACHE_METADATA, true)

    val splits = mutable.ArrayBuffer.empty[ParquetInputSplit]
    val filter: Filter = ParquetInputFormat.getFilter(configuration)
    var rowGroupsDropped: Long = 0
    var totalRowGroups: Long  = 0

    // Ugly hack, stuck with it until PR:
    // https://github.com/apache/incubator-parquet-mr/pull/17
    // is resolved
    val generateSplits =
      Class.forName("parquet.hadoop.ClientSideMetadataSplitStrategy")
       .getDeclaredMethods.find(_.getName == "generateSplits").getOrElse(
         sys.error(s"Failed to reflectively invoke ClientSideMetadataSplitStrategy.generateSplits"))
    generateSplits.setAccessible(true)

    for (footer <- footers) {
      val fs = footer.getFile.getFileSystem(configuration)
      val file = footer.getFile
      val status = fileStatuses.getOrElse(file, fs.getFileStatus(file))
      val parquetMetaData = footer.getParquetMetadata
      val blocks = parquetMetaData.getBlocks
      totalRowGroups = totalRowGroups + blocks.size
      val filteredBlocks = RowGroupFilter.filterRowGroups(
        filter,
        blocks,
        parquetMetaData.getFileMetaData.getSchema)
      rowGroupsDropped = rowGroupsDropped + (blocks.size - filteredBlocks.size)

      if (!filteredBlocks.isEmpty){
          var blockLocations: Array[BlockLocation] = null
          if (!cacheMetadata) {
            blockLocations = fs.getFileBlockLocations(status, 0, status.getLen)
          } else {
            blockLocations = blockLocationCache.get(status, new Callable[Array[BlockLocation]] {
              def call(): Array[BlockLocation] = fs.getFileBlockLocations(status, 0, status.getLen)
            })
          }
          splits.addAll(
            generateSplits.invoke(
              null,
              filteredBlocks,
              blockLocations,
              status,
              readContext.getRequestedSchema.toString,
              readContext.getReadSupportMetadata,
              minSplitSize,
              maxSplitSize).asInstanceOf[JList[ParquetInputSplit]])
        }
    }

    if (rowGroupsDropped > 0 && totalRowGroups > 0){
      val percentDropped = ((rowGroupsDropped/totalRowGroups.toDouble) * 100).toInt
      logInfo(s"Dropping $rowGroupsDropped row groups that do not pass filter predicate "
        + s"($percentDropped %) !")
    }
    else {
      logInfo("There were no row groups that could be dropped due to filter predicates")
    }
    splits

  }

  def getTaskSideSplits(
    configuration: Configuration,
    footers: JList[Footer],
    maxSplitSize: JLong,
    minSplitSize: JLong,
    readContext: ReadContext): JList[ParquetInputSplit] = {

    val splits = mutable.ArrayBuffer.empty[ParquetInputSplit]

    // Ugly hack, stuck with it until PR:
    // https://github.com/apache/incubator-parquet-mr/pull/17
    // is resolved
    val generateSplits =
      Class.forName("parquet.hadoop.TaskSideMetadataSplitStrategy")
       .getDeclaredMethods.find(_.getName == "generateTaskSideMDSplits").getOrElse(
         sys.error(
           s"Failed to reflectively invoke TaskSideMetadataSplitStrategy.generateTaskSideMDSplits"))
    generateSplits.setAccessible(true)

    for (footer <- footers) {
      val file = footer.getFile
      val fs = file.getFileSystem(configuration)
      val status = fileStatuses.getOrElse(file, fs.getFileStatus(file))
      val blockLocations = fs.getFileBlockLocations(status, 0, status.getLen)
      splits.addAll(
        generateSplits.invoke(
         null,
         blockLocations,
         status,
         readContext.getRequestedSchema.toString,
         readContext.getReadSupportMetadata,
         minSplitSize,
         maxSplitSize).asInstanceOf[JList[ParquetInputSplit]])
    }

    splits
  }

}

private[parquet] object FilteringParquetRowInputFormat {
  private val footerCache = CacheBuilder.newBuilder()
    .maximumSize(20000)
    .build[FileStatus, Footer]()

  private val blockLocationCache = CacheBuilder.newBuilder()
    .maximumSize(20000)
    .expireAfterWrite(15, TimeUnit.MINUTES)  // Expire locations since HDFS files might move
    .build[FileStatus, Array[BlockLocation]]()
}

private[parquet] object FileSystemHelper {
  def listFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.exists(path) || !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: path $path does not exist or is not a directory")
    }
    fs.globStatus(path)
      .flatMap { status => if(status.isDir) fs.listStatus(status.getPath) else List(status) }
      .map(_.getPath)
  }

    /**
     * Finds the maximum taskid in the output file names at the given path.
     */
  def findMaxTaskId(pathStr: String, conf: Configuration): Int = {
    val files = FileSystemHelper.listFiles(pathStr, conf)
    // filename pattern is part-r-<int>.parquet
    val nameP = new scala.util.matching.Regex("""part-r-(\d{1,}).parquet""", "taskid")
    val hiddenFileP = new scala.util.matching.Regex("_.*")
    files.map(_.getName).map {
      case nameP(taskid) => taskid.toInt
      case hiddenFileP() => 0
      case other: String =>
        sys.error("ERROR: attempting to append to set of Parquet files and found file" +
          s"that does not match name pattern: $other")
      case _ => 0
    }.reduceOption(_ max _).getOrElse(0)
  }
}
