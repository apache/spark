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

package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit._

// <<<<<<< HEAD
// =======
// import scala.collection.mutable
// import scala.collection.mutable.{ArrayBuffer, Queue}

// // scalastyle:off import.ordering.noEmptyLine
// import com.databricks.photon.{CloudFileNotFoundHintGenerator, DefaultFileNotFoundHintGenerator, DeltaFileNotFoundHintGenerator, FileNotFoundHintGenerator}
// import com.databricks.spark.metrics.DiskCacheMetrics
// import com.databricks.spark.util.FrameProfiler
// import com.databricks.spark.util.UsageLogging

// import com.databricks.sql.DatabricksSQLConf
// import com.databricks.sql.catalyst.BadFilesWriter
// import com.databricks.sql.execution.{AutoTuneOptimizedWrites, FileScanMetrics, FileSystemMetrics, OptimizeBypassDeltaCache, RepeatedReadsMetrics, SQLFileScanMetrics}
// import com.databricks.sql.execution.adaptive.CancelledScanNodeCache.removeNewDynamicPruningFilters
// import com.databricks.sql.execution.metric.IncrementDynamicPruningMetricEdge // Edge
// import com.databricks.sql.expressions.runtimefilters.RangeBloomFilterMightContainValue // Edge
// import com.databricks.sql.io.RowIndexFilterProvider
// import com.databricks.sql.io.caching.{CachingUtils, ParquetLocalityManager, RepeatedReadsEstimator}
// import com.databricks.sql.io.memcache.MemCacheFileFormat
// import com.databricks.sql.io.skipping.BloomFilterFileFormat
// import com.databricks.sql.managedcatalog.DbStorageRedactionUtils
// import com.databricks.sql.transaction.tahoe.{DeltaErrors, GeneratedColumn}
// import com.databricks.sql.transaction.tahoe.commands.cdc.CDCReader
// import com.databricks.sql.transaction.tahoe.files.{SupportsRowIndexFilters, TahoeFileIndex}
// import com.databricks.sql.transaction.tahoe.files.TahoeFileIndexWithStaticPartitions
// import com.databricks.sql.transaction.tahoe.stats.FileSizeHistogram
// import com.databricks.sql.transaction.tahoe.stats.FileSizeHistogramUtils // Edge
// import com.databricks.sql.transaction.tahoe.util.BitmapFilter

// import io.delta.sharing.spark.DeltaSharingFileIndex // Edge
// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.{truncatedString, CaseInsensitiveMap}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.datasources.v2.PushedDownOperators
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

trait DataSourceScanExec extends LeafExecNode {
  def relation: BaseRelation
  def tableIdentifier: Option[TableIdentifier]

  protected val nodeNamePrefix: String = ""

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // Metadata that describes more details of this scan.
  protected def metadata: Map[String, String]

  protected val maxMetadataValueLength = conf.maxMetadataStringLength

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr")
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  /**
   * Shorthand for calling redactString() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(conf.stringRedactionPattern, text)
  }

  /**
   * The data being read in.  This is to provide input to the tests in a way compatible with
   * [[InputRDDCodegen]] which all implementations used to extend.
   */
  def inputRDDs(): Seq[RDD[InternalRow]]
}

object DataSourceScanExec {
  val numOutputRowsKey = "numOutputRows"
}

/** Physical plan node for scanning data from a relation. */
case class RowDataSourceScanExec(
    output: Seq[Attribute],
    requiredSchema: StructType,
    filters: Set[Filter],
    handledFilters: Set[Filter],
    pushedDownOperators: PushedDownOperators,
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with InputRDDCodegen {

  override lazy val metrics: Map[String, SQLMetric] = {
    val metrics = Map(
      DataSourceScanExec.numOutputRowsKey ->
        SQLMetrics.createMetric(sparkContext, "number of output rows")
    )

    rdd match {
      case rddWithDSMetrics: DataSourceMetricsMixin => metrics ++ rddWithDSMetrics.getMetrics
      case _ => metrics
    }
  }

  override def verboseStringWithOperatorId(): String = {
    super.verboseStringWithOperatorId() + (rdd match {
      case externalEngineDatasourceRdd: ExternalEngineDatasourceRDD =>
        "External engine query: " +
          externalEngineDatasourceRdd.getExternalEngineQuery +
          System.lineSeparator()
      case _ => ""
    })
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map( r => {
        numOutputRows += 1
        proj(r)
      })
    }
  }

  // Input can be InternalRow, has to be turned into UnsafeRows.
  override protected val createUnsafeProjection: Boolean = true

  override def inputRDD: RDD[InternalRow] = rdd

  override val metadata: Map[String, String] = {

    def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

    val markedFilters = if (filters.nonEmpty) {
      for (filter <- filters) yield {
        if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
      }
    } else {
      handledFilters
    }

    val topNOrLimitInfo =
      if (pushedDownOperators.limit.isDefined && pushedDownOperators.sortValues.nonEmpty) {
        val pushedTopN =
          s"ORDER BY ${seqToString(pushedDownOperators.sortValues.map(_.describe()))}" +
          s" LIMIT ${pushedDownOperators.limit.get}"
        Some("PushedTopN" -> pushedTopN)
      } else {
        pushedDownOperators.limit.map(value => "PushedLimit" -> s"LIMIT $value")
      }

    val offsetInfo = pushedDownOperators.offset.map(value => "PushedOffset" -> s"OFFSET $value")

    val pushedFilters = if (pushedDownOperators.pushedPredicates.nonEmpty) {
      seqToString(pushedDownOperators.pushedPredicates.map(_.describe()))
    } else {
      seqToString(markedFilters.toSeq)
    }

    Map("ReadSchema" -> requiredSchema.catalogString,
      "PushedFilters" -> pushedFilters) ++
      pushedDownOperators.aggregation.fold(Map[String, String]()) { v =>
        Map("PushedAggregates" -> seqToString(
          v.aggregateExpressions.map(_.describe()).toImmutableArraySeq),
          "PushedGroupByExpressions" ->
            seqToString(v.groupByExpressions.map(_.describe()).toImmutableArraySeq))} ++
      topNOrLimitInfo ++
      offsetInfo ++
      pushedDownOperators.sample.map(v => "PushedSample" ->
        s"SAMPLE (${(v.upperBound - v.lowerBound) * 100}) ${v.withReplacement} SEED(${v.seed})"
      )
  }

  // Don't care about `rdd` and `tableIdentifier` when canonicalizing.
  override def doCanonicalize(): SparkPlan =
    copy(
      output.map(QueryPlan.normalizeExpressions(_, output)),
      rdd = null,
      tableIdentifier = None)
}

/**
 * A base trait for file scans containing file listing and metrics code.
 */
trait FileSourceScanLike extends DataSourceScanExec {

  // Filters on non-partition columns.
  def dataFilters: Seq[Expression]
  // Disable bucketed scan based on physical query plan, see rule
  // [[DisableUnnecessaryBucketedScan]] for details.
  def disableBucketedScan: Boolean
  // Bucket ids for bucket pruning.
  def optionalBucketSet: Option[BitSet]
  // Number of coalesced buckets.
  def optionalNumCoalescedBuckets: Option[Int]
  // Output attributes of the scan, including data attributes and partition attributes.
  def output: Seq[Attribute]
  // Predicates to use for partition pruning.
  def partitionFilters: Seq[Expression]
  // The file-based relation to scan.
  def relation: HadoopFsRelation
  // Required schema of the underlying relation, excluding partition columns.
  def requiredSchema: StructType
  // Identifier for the table in the metastore.
  def tableIdentifier: Option[TableIdentifier]


  lazy val fileConstantMetadataColumns: Seq[AttributeReference] = output.collect {
    // Collect metadata columns to be handled outside of the scan by appending constant columns.
    case FileSourceConstantMetadataAttribute(attr) => attr
  }

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf).map { vectorTypes =>
        vectorTypes ++
          // for column-based file format, append metadata column's vector type classes if any
          fileConstantMetadataColumns.map { _ => classOf[ConstantColumnVector].getName }
      }

  lazy val driverMetrics = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read")
  ) ++ {
    if (relation.partitionSchema.nonEmpty) {
      Map(
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
        "pruningTime" ->
          SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"))
    } else {
      Map.empty[String, SQLMetric]
    }
  } ++ staticMetrics

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  protected def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, driverMetrics.values.toSeq)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

// <<<<<<< HEAD

  // This field will be accessed during planning (e.g., `outputPartitioning` relies on it), and can
  // only use static filters.
  @transient lazy val selectedPartitions: ScanFileListing = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    // The filters may contain subquery expressions which can't be evaluated during planning.
    // Here we filter out subquery expressions and get the static data/partition filters, so that
    // they can be used to do pruning at the planning phase.
    val staticDataFilters = dataFilters.filterNot(isDynamicPruningFilter)
    val staticPartitionFilters = partitionFilters.filterNot(isDynamicPruningFilter)
    val partitionDirectories = relation.location.listFiles(
      staticPartitionFilters, staticDataFilters)
    val fileListing = GenericScanFileListing(partitionDirectories.toArray)
    setFilesNumAndSizeMetric(fileListing, static = true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime").set(timeTakenMs)
    fileListing
  }
// =======
//   def hasDynamicFileFilters(): Boolean = {
//     dataFilters.exists(DynamicPruning.isDynamicFilter)
//   }

//   /**
//    * Use the result keys of a broadcast exchange to construct dynamic file pruning filters:
//    *  (1) if the number of keys is less a given threshold, then use an In list expression
//    *  (2) otherwise, create a min-max range on the elements of the In list.
//    */
//   private def getFilePruningFilterFromBroadcast(
//       dynamicFileFilters: Seq[Expression]): Seq[Expression] = {
//     dynamicFileFilters.collect {
//       // match dynamic pruning expressions that incorporate the broadcast results
//       case DynamicPruningExpression(in: InSubqueryExec) =>
//         in.getFilterPredicate match {
//           case InSet(child, hset) =>
//             val numKeptMetric = IncrementDynamicPruningMetricEdge(
//               Literal.TrueLiteral, driverMetrics("dfpNumFilesKept"))
//             val numPrunedMetric = IncrementDynamicPruningMetricEdge(
//               Literal.FalseLiteral, driverMetrics("dfpNumFilesPruned"))
//             If(InSet(child, hset), numKeptMetric, numPrunedMetric)
//           case other => other
//         }
//       case r: RangeBloomFilterMightContainValue => r // Edge
//     }
//   }

//   /**
//    * Unwrap DynamicPruningExpression around InSubqueryExec. Compared to unwrapping dynamic file
//    * filters, other dynamic filters are kept and not dropped.
//    * */
//   private def getPartitionPruningFilterFromBroadcast(
//       dynamicPartitionFilters: Seq[Expression]): Seq[Expression] = {
//     dynamicPartitionFilters.map { filter =>
//       filter.transformDown {
//         case DynamicPruningExpression(in: execution.InSubqueryExec) =>
//           in.getFilterPredicate match {
//             case InSet(child, hset) =>
//               // The file skipping predicate is applied per file in Delta, and per partition for the
//               // other formats.
//               val measurementUnit = if (isDeltaSource) "Files" else "Partitions"
//               val numKeptMetric = IncrementDynamicPruningMetricEdge(
//                 Literal.TrueLiteral, driverMetrics(s"dppNum${measurementUnit}Kept"))
//               val numFPrunedMetric = IncrementDynamicPruningMetricEdge(
//                 Literal.FalseLiteral, driverMetrics(s"dppNum${measurementUnit}Pruned"))
//               If(InSet(child, hset), numKeptMetric, numFPrunedMetric)
//             case other => other
//           }
//         case in: execution.InSubqueryExec => in.getFilterPredicate
//         case DynamicPruningExpression(scalar: execution.ScalarSubquery) => scalar.toLiteral
//         case scalar: execution.ScalarSubquery => scalar.toLiteral
//       }
//     }
//   }

//   /**
//    * List source relation location files.
//    * Records sorting time with preserve input order mode and delta source.
//    */
//   private def listFiles(
//       partitionFilters: Seq[Expression],
//       dataFilters: Seq[Expression]): ScanFileListing = {
//     val (partitionDirectories, _) = if (shouldSortSourceFilesByInsertionTime) {
//       // Safe to cast because a delta source should have TahoeFileIndex as location
//       val fileListingResult = relation.location.asInstanceOf[TahoeFileIndex]
//         .listFilesSortedByInsertionTime(partitionFilters, dataFilters)
//       driverMetrics(FileSourceScanExec.FILE_INSERTION_ORDER_SORTING_TIME_METRIC)
//         .set(fileListingResult.sortTime)
//       (fileListingResult.partitionDirectories, fileListingResult.addFiles)
//     } else {
//       val fileListingResult = relation.location
//         .listPartitionDirectoriesAndFiles(partitionFilters, dataFilters)
//       (fileListingResult.partitionDirectories, fileListingResult.addFiles)
//     }

//     // BEGIN-EDGE
//     relation.location match {
//       case index: TahoeFileIndex =>
//         val numOfFilesRead = partitionDirectories.map(_.files.size.toLong).sum
//         val filesSizeBytesRead = partitionDirectories.map(_.files.map(_.getLen).sum).sum
//         val (numOfFilesPruned, filesSizeBytesPruned) =
//          index.getNumOfFilesAndBytesPruned(
//             numOfFilesRead = numOfFilesRead, filesSizeBytesRead = filesSizeBytesRead)
//         driverMetrics("numFilesPruned").set(numOfFilesPruned)
//         driverMetrics("filesSizeBytesPruned").set(filesSizeBytesPruned)
//       case _ =>
//     }
//     // END-EDGE
//     GenericScanFileListing(partitionDirectories.toArray)
//   }

//   // This field will be accessed during planning (e.g., `outputPartitioning` relies on it), and can
//   // only use static filters.
//   // Edge: Lazy on purpose, so that we don't needlessly call listFiles() every time a canonicalized
//   // plan is computed (and a new FileSourceScanExec is instantiated). See SC-7951.
//   @transient lazy val selectedPartitions: ScanFileListing =
//     reusesFileListingResultsSourceNode.map(_.selectedPartitions).getOrElse({
//       val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
//       val startTime = System.nanoTime()
//       // The filters may contain subquery expressions which can't be evaluated during planning.
//       // Here we filter out subquery expressions and get the static data/partition filters, so that
//       // they can be used to do pruning at the planning phase.
//       val staticDataFilters = dataFilters.filterNot(DynamicPruning.isDynamicFilter)
//       val staticPartitionFilters = partitionFilters.filterNot(DynamicPruning.isDynamicFilter)
//       val fileListing = listFiles(staticPartitionFilters, staticDataFilters)
//       setDriverMetrics(fileListing, static = true)
//       val timeTakenMs = NANOSECONDS.toMillis(
//         (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
//       driverMetrics("metadataTime").set(timeTakenMs)
//       fileListing
//     })
// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
// <<<<<<< HEAD
  @transient protected lazy val dynamicallySelectedPartitions: ScanFileListing = {
    val dynamicDataFilters = dataFilters.filter(isDynamicPruningFilter)
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      // var ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      // if (dynamicDataFilters.nonEmpty) {
      //   val filePruningRunner = new FilePruningRunner(dynamicDataFilters)
      //   ret = ret.map(filePruningRunner.prune)
      val returnedFiles = selectedPartitions.filterAndPruneFiles(
        boundPredicate, dynamicDataFilters)
// =======
//   @transient protected lazy val dynamicallySelectedPartitions: ScanFileListing =
//   FrameProfiler.record("DataSourceScanExec", "dynamicallySelectedPartitions") {
//     reusesFileListingResultsSourceNode.map(_.dynamicallySelectedPartitions).getOrElse {
//       val dynamicPartitionFilters = partitionFilters.filter(DynamicPruning.isDynamicFilter)
//       val staticPartitionFilters = partitionFilters.filterNot(DynamicPruning.isDynamicFilter)
//       val dynamicFileFilters = dataFilters.filter(DynamicPruning.isDynamicFilter)
//       val staticFileFilters = dataFilters.filterNot(DynamicPruning.isDynamicFilter)

//       if (dynamicPartitionFilters.nonEmpty || dynamicFileFilters.nonEmpty) {
//         val startTime = System.nanoTime()

//         val nativeDynamicPartitionPruningEnabled =
//           conf.getConf(DatabricksSQLConf.Optimizer.DYNAMIC_PARTITION_PRUNNING_IN_FILE_INDEX_ENABLED)
//         val finalFiles = if (nativeDynamicPartitionPruningEnabled) {
//           val finalFileFilters = staticFileFilters ++
//             getFilePruningFilterFromBroadcast(dynamicFileFilters)
//           val finalPartitionFilters = staticPartitionFilters ++
//             getPartitionPruningFilterFromBroadcast(dynamicPartitionFilters)
//           val fileListingResult = listFiles(finalPartitionFilters, finalFileFilters)
//           setDriverMetrics(fileListingResult, static = false)
//           fileListingResult
//         } else {
//           // rewrite dynamic file filters using the broadcast results
//           val files = if (dynamicFileFilters.nonEmpty) {
//             val finalFileFilters = staticFileFilters ++
//               getFilePruningFilterFromBroadcast(dynamicFileFilters)
//             // Get the files matching all filters except dynamic partition filters.
//             val fileListingResult = listFiles(staticPartitionFilters, finalFileFilters)
//             setDriverMetrics(fileListingResult, static = true)
//             fileListingResult
//           } else {
//             selectedPartitions
//           }

//           if (dynamicPartitionFilters.nonEmpty) {
//             // dynamic partition filters are evaluated separately as they can be
//             // evaluated directly without rewriting
//             val predicate = dynamicPartitionFilters.reduce(And)
//             val partitionColumns = relation.partitionSchema
//             val boundPredicate = Predicate.create(predicate.transform {
//               case a: AttributeReference =>
//                 val index = partitionColumns.indexWhere(a.name == _.name)
//                 BoundReference(index, partitionColumns(index).dataType, nullable = true)
//             }, Nil)
//             val returnedFiles = files.filterAndPruneFiles(boundPredicate, dynamicFileFilters)
//             setDriverMetrics(returnedFiles, static = false)
//             returnedFiles
//           } else {
//             files
//           }
//         }
//         val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
//         driverMetrics("pruningTime").set(timeTakenMs)
//         finalFiles
//       } else {
//         selectedPartitions
// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)
      // }
      setFilesNumAndSizeMetric(returnedFiles, false)
      val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
      driverMetrics("pruningTime").set(timeTakenMs)
      returnedFiles
    } else {
      selectedPartitions
    }
  }

// <<<<<<< HEAD
// =======
   /**
    * Returns the number of selected partitions.
    */
   def selectedPartitionCount: Int = selectedPartitions.partitionCount

//   /**
//    * Returns the number of dynamically selected partitions.
//    */
//   def finalSelectedPartitionCount: Int = finalSelectedPartitions.partitionCount

  // // BEGIN-EDGE
  // // These are Edge-only utility functions for the time being.
  // // They will be replaced by a follow-up PR, which will also be backported to OSS.
  // /**
  //  * Calculates the total size in bytes of all files across the selected partitions.
  //  */
  // lazy val totalSelectedPartitionFileSize: Long = selectedPartitions.totalFileSize

  // /**
  //  * Calculates the total size in bytes of all files across the dynamically selected partitions.
  //  */
  // lazy val totalFinalSelectedPartitionFileSize: Long = finalSelectedPartitions.totalFileSize
  // // END-EDGE

   /**
    * Calculates the total number of files across the selected partitions.
    */
   lazy val totalNumOfFilesInSelectedPartitions: Long = selectedPartitions.totalNumberOfFiles

  // /**
  //  * Returns the dynamically selected partitions.
  //  */
  // def finalSelectedPartitions: ScanFileListing = dynamicallySelectedPartitions

// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)
  private def toAttribute(colName: String): Option[Attribute] =
    output.find(_.name == colName)

  // exposed for testing
  lazy val bucketedScan: Boolean = {
    if (relation.sparkSession.sessionState.conf.bucketingEnabled && relation.bucketSpec.isDefined
      && !disableBucketedScan) {
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      bucketColumns.size == spec.bucketColumnNames.size
    } else {
      false
    }
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    if (bucketedScan) {
      // For bucketed columns:
      // -----------------------
      // `HashPartitioning` would be used only when:
      // 1. ALL the bucketing columns are being read from the table
      //
      // For sorted columns:
      // ---------------------
      // Sort ordering should be used when ALL these criteria's match:
      // 1. `HashPartitioning` is being used
      // 2. A prefix (or all) of the sort columns are being read from the table.
      //
      // Sort ordering would be over the prefix subset of `sort columns` being read
      // from the table.
      // e.g.
      // Assume (col0, col2, col3) are the columns read from the table
      // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
      // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
      // above
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      val numPartitions = optionalNumCoalescedBuckets.getOrElse(spec.numBuckets)
      val partitioning = HashPartitioning(bucketColumns, numPartitions)
      val sortColumns =
        spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)
      val shouldCalculateSortOrder =
        conf.getConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING) &&
          sortColumns.nonEmpty

      val sortOrder = if (shouldCalculateSortOrder) {
        // In case of bucketing, its possible to have multiple files belonging to the
        // same bucket in a given relation. Each of these files are locally sorted
        // but those files combined together are not globally sorted. Given that,
        // the RDD partition will not be sorted even if the relation has sort columns set
        // Current solution is to check if all the buckets have a single file in it
        val singleFilePartitions = selectedPartitions.bucketsContainSingleFile

        // TODO SPARK-24528 Sort order is currently ignored if buckets are coalesced.
        if (singleFilePartitions && optionalNumCoalescedBuckets.isEmpty) {
          // TODO Currently Spark does not support writing columns sorting in descending order
          // so using Ascending order. This can be fixed in future
          sortColumns.map(attribute => SortOrder(attribute, Ascending))
        } else {
          Nil
        }
      } else {
        Nil
      }
      (partitioning, sortOrder)
    } else {
      (UnknownPartitioning(0), Nil)
    }
  }

  private def translateToV1Filters(
      dataFilters: Seq[Expression],
      scalarSubqueryToLiteral: execution.ScalarSubquery => Literal): Seq[Filter] = {
    val scalarSubqueryReplaced = dataFilters.map(_.transform {
      // Replace scalar subquery to literal so that `DataSourceStrategy.translateFilter` can
      // support translating it.
      case scalarSubquery: execution.ScalarSubquery => scalarSubqueryToLiteral(scalarSubquery)
    })

    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    // `dataFilters` should not include any constant metadata col filters
    // because the metadata struct has been flatted in FileSourceStrategy
    // and thus metadata col filters are invalid to be pushed down. Metadata that is generated
    // during the scan can be used for filters.
    scalarSubqueryReplaced.filterNot(_.references.exists {
      case FileSourceConstantMetadataAttribute(_) => true
      case _ => false
    }).flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  // This field may execute subquery expressions and should not be accessed during planning.
  @transient
  protected lazy val pushedDownFilters: Seq[Filter] = translateToV1Filters(dataFilters, _.toLiteral)

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName +
        Utils.buildLocationMetadata(location.rootPaths, maxMetadataValueLength)
    // `metadata` is accessed during planning and the scalar subquery is not executed yet. Here
    // we get the pretty string of the scalar subquery, for display purpose only.
    val pushedFiltersForDisplay = translateToV1Filters(
      dataFilters, s => Literal("ScalarSubquery#" + s.exprId.id))
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsColumnar.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedFiltersForDisplay),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)

    relation.bucketSpec.map { spec =>
      val bucketedKey = "Bucketed"
      if (bucketedScan) {
        val numSelectedBuckets = optionalBucketSet.map { b =>
          b.cardinality()
        } getOrElse {
          spec.numBuckets
        }
        metadata ++ Map(
          bucketedKey -> "true",
          "SelectedBucketsCount" -> (s"$numSelectedBuckets out of ${spec.numBuckets}" +
            optionalNumCoalescedBuckets.map { b => s" (Coalesced to $b)"}.getOrElse("")))
      } else if (!relation.sparkSession.sessionState.conf.bucketingEnabled) {
        metadata + (bucketedKey -> "false (disabled by configuration)")
      } else if (disableBucketedScan) {
        metadata + (bucketedKey -> "false (disabled by query planner)")
      } else {
        metadata + (bucketedKey -> "false (bucket column(s) not read)")
      }
    } getOrElse {
      metadata
    }
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLocation = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLocation)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  override def metrics: Map[String, SQLMetric] = scanMetrics

// <<<<<<< HEAD
// =======
//   lazy val scanFileSizeHistogram: FileSizeHistogram = FileSizeHistogramUtils.emptyHistogram // Edge

//   /**
//    * Builds a default Hadoop configuration for the input RDD.
//    */
//   protected def buildHadoopConfForInputRDD(): Configuration = FrameProfiler.record(
//     "DataSourceScanExec", "buildHadoopConfForInputRDD") {
//     val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
//     // Explicitly mark if it's a delta table so that we can skip
//     // the staleness check in CachingParquetFileReader
//     if (isDeltaSource) {
//       hadoopConf.setBoolean(DatabricksSQLConf.IO_CACHE_PARQUET_IS_DELTA_SOURCE.key, true)
//     }
//     // Mark that this will be a bucketed read so that later we can split each bucketed file by
//     // `conf.filesMaxPartitionBytes` and create keys for caching for each split.
//     if (relation.sparkSession.conf.get(DatabricksSQLConf.BUCKETED_IO_CACHE_ENABLED)
//         && bucketedScan) {
//       hadoopConf.setLong(
//         ParquetLocalityManager.BUCKETED_SCAN_MAX_PARTITION_BYTES_KEY, conf.filesMaxPartitionBytes)
//     }
//     hadoopConf.setBoolean(
//       OptimizeBypassDeltaCache.OPTIMIZE_BYPASS_DELTA_CACHE_MODE, bypassDeltaCacheForOptimize)
//     hadoopConf
//   }

//   @transient lazy val hadoopConf = FrameProfiler.record(
//     "SparkOrAetherPhotonParquetFileScanLike", "hadoopConf") {
//     relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
//   }


//   /**
//    * Decide whether Parquet caching should be used in given scan.
//    * This is done by looking at any of the files to be scanned, and checking it's prefix.
//    */
//   protected lazy val useParquetCaching: Boolean = {
//     if (useParquetCachingByConf) {
//       val sqlConf = relation.sparkSession.sessionState.conf
//       val prefixes = sqlConf.getConf(DatabricksSQLConf.IO_CACHE_PREFIX)
//       val blacklist = sqlConf.getConf(DatabricksSQLConf.IO_CACHE_BLACKLIST_REGEX).orNull

//       dynamicallySelectedPartitions.shouldCachePath(prefixes, blacklist)
//     } else {
//       false
//     }
//   }

//   /**
//    * Should the RepeatedReadsEstimator by used for files scanned by this this operator?
//    * For now, we're only leveraging it for Parquet files to estimate IO cache effectiveness,
//    * but it could also serve to detect intensive use of data in slow file formats (e.g. Json).
//    */
//   protected lazy val computeEstRepeatedReads: Boolean = relation.sparkSession.sessionState.conf
//     .getConf(DatabricksSQLConf.IO_CACHE_ESTIMATE_REPEATED_READS) && isParquetSource

//   // BEGIN-EDGE
//   // This enables redaction of _metadata column values based on properties of the file format
//   // and the logical relation. For Arclight (Databricks storage) tables, we redact the file path.
//   protected def getFileConstantMetadataExtractors: Map[String, PartitionedFile => Any] = {
//     val extractors = relation.fileFormat.fileConstantMetadataExtractors
//     // logical relation may be null after canonicalization
//     val securableKindOpt = Option(logicalRelation).flatMap(_.catalogTable.flatMap(_.securableKind))
//     if (DbStorageRedactionUtils.isDbStorageSecurableKind(securableKindOpt)
//       && extractors.contains(FileFormat.FILE_PATH)) {
//       val redactedExtractor = (_: PartitionedFile) =>
//         DbStorageRedactionUtils.REDACTED_DB_STORAGE_LOCATION
//       extractors.updated(FileFormat.FILE_PATH, redactedExtractor)
//     } else {
//       extractors
//     }
//   }
//   // END-EDGE

//   protected def getRepeatedReadsMetrics: Option[RepeatedReadsMetrics] = {
//     if (computeEstRepeatedReads) {
//       Some(new RepeatedReadsMetrics(
//         longMetric("estRepeatedReadsLo"),
//         longMetric("estRepeatedReadsHi"))
//       )
//     } else {
//       None
//     }
//   }

// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)
  /** SQL metrics generated only for scans using dynamic partition pruning. */
  protected lazy val staticMetrics = if (partitionFilters.exists(isDynamicPruningFilter)) {
    Map("staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
      "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read"))
  } else {
    Map.empty[String, SQLMetric]
  }

// <<<<<<< HEAD
  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(partitions: ScanFileListing, static: Boolean): Unit = {
    val filesNum = partitions.totalNumberOfFiles
    val filesSize = partitions.totalFileSize
    // val filesNum = partitions.map(_.files.size.toLong).sum
    // val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
// =======
//   private lazy val dynamicPartitionPruningMetrics: Map[String, SQLMetric] = {
//     if (partitionFilters.exists(DynamicPruning.isDynamicFilter)) {
//       if (isDeltaSource) {
//         Map(
//           createNamedCountMetric("dppNumFilesKept", experimental = true),
//           createNamedCountMetric("dppNumFilesPruned", experimental = true)
//         )
//       } else {
//         Map(
//           createNamedCountMetric("dppNumPartitionsKept", experimental = true),
//           createNamedCountMetric("dppNumPartitionsPruned", experimental = true)
//         )
//       }
//     } else {
//       Map.empty
//     }
//   }

//   private lazy val dynamicFilePruningMetrics: Map[String, SQLMetric] = {
//     if (dataFilters.exists(DynamicPruning.isDynamicFilter)) {
//       Map(
//         createNamedCountMetric("dfpNumFilesKept", experimental = true),
//         createNamedCountMetric("dfpNumFilesPruned", experimental = true)
//       )
//     } else {
//       Map.empty
//     }
//   }
//   // END-EDGE

//   /** SQL metrics generated only for scans involving autoTuneOptimizedWrites. */
//   protected lazy val autoTuneOptimizedWriteMetrics =
//     Map(
//       "prorationFactor" ->
//         SQLMetrics.createMetric(sparkContext, "the proration factor used"),
//       "maxPartitionBytes" ->
//         SQLMetrics.createSizeMetric(sparkContext, "max partition size chosen"),
//       "clusterParallelism" ->
//         SQLMetrics.createMetric(sparkContext, "cluster's current parallelism ")
//     )

//   protected def setAutoTuneOptimizedWriteMetrics(
//     prorationFactor: Float, maxPartitionBytes: Long, clusterParallelism: Int): Unit = {
//     if (autoTuneOptimizedWritesEnabled) {
//       driverMetrics("prorationFactor").set(prorationFactor.toInt)
//       driverMetrics("maxPartitionBytes").set(maxPartitionBytes)
//       driverMetrics("clusterParallelism").set(clusterParallelism)
//     }
//   }


//   /**
//    * Helper for computing a number of driver metrics. These include:
//    * 1. Total number of partitions
//    * 2. Total number and size of files in selected partitions.
//    *   - The `staticFilesNum` and the `staticFilesSize` metrics are set only if there are dynamic
//    *     partition pruning filters, and represent the metrics before dynamic partition pruning is
//    *     applied... and in that case `numFiles` and `filesSize` represent metrics after dynamic
//    *     partition pruning is applied.
//    *   - Note that the `static` metrics however are set after dynamic file pruning (if DFP filters
//    *     exist). So if there are 10 files, but 2 are filtered away by DFP and 3 are filtered away
//    *     by DPP, we will see staticFilesNum = 8 and numFiles = 5.
//    *   - If Dynamic Partition Pruning filters don't exist, then `numFiles` and `filesSize` represent
//    *     the file metrics.
//    *   - If DPP and DFP filters are bypassed at runtime due to exceeding size thresholds, the
//    *     metrics behave as if the filters existed, but did not filter anything. So for example,
//    *     in case of DPP, we will see that the `staticFilesNum` will be the same as `numFiles` if
//    *     the DPP filters were bypassed at runtime.
//    * 3. Size distribution of selected files (histogram and min/max file size)
//    * 4. Metrics related to column information (see setColumnMetrics())
//    */
//   private def setDriverMetrics(partitions: ScanFileListing, static: Boolean): Unit = {
//     driverMetrics("preferredLocationsVersion").set(if (plv2Enabled) 2 else 1)
//     // Use iterator to avoid materialization of the whole file list for each partition.
//     val (fileSizeIterator: Iterator[Long], filesNum) = partitions.fileSizeIteratorAndFileCount
//     if (!static || !partitionFilters.exists(DynamicPruning.isDynamicFilter)) {
//       // This is a hack to prevent `numFiles` set by `dynamicallySelectedPartitions` being
//       // overwritten by `selectedPartitions`.
//       if (!driverMetrics.get("numFiles").map(_.value).exists(n => n > 0 && n <= filesNum)) {
//         driverMetrics("numFiles").set(filesNum)
//         var filesSize = 0L
//         // Only needs to calculate min and max file sizes if there is at least one file
//         if (filesNum > 0) {
//           var maxFileSize = 0L
//           var minFileSize = Long.MaxValue
//           fileSizeIterator.foreach { fileSize =>
//             filesSize += fileSize
//             maxFileSize = maxFileSize.max(fileSize)
//             minFileSize = minFileSize.min(fileSize)
//             scanFileSizeHistogram.insert(fileSize) // Edge
//           }
//           driverMetrics("minFileSize").set(minFileSize)
//           driverMetrics("maxFileSize").set(maxFileSize)
//         }
//         driverMetrics("filesSize").set(filesSize)
//       }
//     } else {
//       driverMetrics("staticFilesNum").set(filesNum)
//       driverMetrics("staticFilesSize").set(fileSizeIterator.sum)
// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions").set(partitions.partitionCount)
    }
  }

  private lazy val scanMetrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
// <<<<<<< HEAD
  } ++ driverMetrics
// =======
//   } ++ {
//     if (isParquetSource) {
//       Some("parquetFileReadBy" ->
//         SQLMetrics.createV2CustomMetric(sparkContext, new ParquetFileReadMetrics()))
//     } else {
//       None
//     }
//   } ++ {
//     Map(
//       FileSourceScanLike.MISSING_FILES_METRIC ->
//         SQLMetrics.createMetric(sparkContext, "missing files"),
//       FileSourceScanLike.CORRUPT_FILES_METRIC ->
//         SQLMetrics.createMetric(sparkContext, "corrupt files"))
//   } ++  cacheExecutorMetrics ++ bloomFilterSkippingMetrics ++ repeatedReadsMetrics ++
//     prefetchingMetrics ++ rowGroupMetrics ++ dvMetrics ++ cloudStorageMetrics ++ driverMetrics ++
//     diskCacheMetrics

//   // Metrics of a collection of data: corrupt file paths, missing file paths
//   lazy val collectionMetrics: Map[String, CollectionAccumulator[String]] = {
//     def createCollectionMetric(sc: SparkContext, name: String): CollectionAccumulator[String] = {
//       val acc = new CollectionAccumulator[String]
//       acc.register(sc, name = Option(name), countFailedValues = false)
//       acc
//     }
//     Map(
//       FileSourceScanExec.METRIC_CORRUPT_FILE_PATHS ->
//         createCollectionMetric(sparkContext, "corrupt file paths"),
//       FileSourceScanExec.METRIC_MISSING_FILE_PATHS ->
//         createCollectionMetric(sparkContext, "missing file paths")
//     )
//   }

//   private def isInternalComputedField(field: StructField): Boolean =
//     Seq(
//       DatabricksSpecificParquetRecordReaderBase.ROW_INDEX_COLUMN_NAME,
//       DatabricksSpecificParquetRecordReaderBase.SKIP_COLUMN_NAME).contains(field.name)

//   /**
//    * Returns a sequence of absolute paths for the files contained in the selected partitions.
//    */
//   def selectedPartitionFilesAbsolutePaths: Seq[String] =
//     selectedPartitions.selectedPartitionFilesAbsolutePaths

//   /**
//    * This trait represents the scan file listing representation. The methods defined in this trait
//    * are describing behavior that is common across all scan file listing representations.
//    *
//    * As mentioned in the `ScanFileListing` trait documentation, this `InnerScanFileListing` trait
//    * is defined nested within the `SparkOrAetherFileSourceScanLike` trait because it needs direct
//    * access to the variables and methods of `SparkOrAetherFileSourceScanLike`. By providing this
//    * direct access, we eliminate the need for redundant boilerplate code, and we encapsulate the
//    * scan file listing representations with the scanning logic, streamlining interactions and
//    * data flow.
//    */
//   private sealed trait InnerScanFileListing extends ScanFileListing {
//     /**
//      * Returns the max number of bytes per partition from the auto tune optimized writes
//      * perspective for the current scan file listing representation.
//      * @return The max number of bytes per partition.
//      */
//     def getMaxPartitionBytesForAutoTuneOptimizedWrites: Long = {
//       val totalBytes = calculateTotalPartitionBytes
//       val conf = relation.sparkSession.sessionState.conf
//       val parallelismFactor =
//         conf.getConf(DatabricksSQLConf.FILES_PRORATE_MAX_PARTITION_BYTES_PARALLELISM_FACTOR)
//       val proratedMaxPartitionBytesUpperBound =
//         conf.getConf(DatabricksSQLConf.FILES_PRORATE_MAX_PARTITION_BYTES_UBOUND)
//       val defaultParallelism =
//         relation.sparkSession.sharedState.adaptiveParallelism.defaultParallelism
//       val defaultMaxSplitBytesConf = conf.filesMaxPartitionBytes
//       val relationFields = relation.dataSchema.fields

//       // Proration is done only if the number of tasks exceed [[defaultParallelism]] by a factor of
//       // [[parallelismFactor]]. Since we are aiming for better file sizes, we always allow proration
//       // in case of autoTuneOptimizedWrites.
//       val allowProration =
//         autoTuneOptimizedWritesEnabled ||
//             totalBytes >= defaultMaxSplitBytesConf * defaultParallelism * parallelismFactor

//       val outputFields = requiredSchema.fields.filterNot(isInternalComputedField)
//       val prorationFactor =
//         if (conf.getConf(DatabricksSQLConf.FILES_PRORATE_MAX_PARTITION_BYTES_ENABLED)
//           && relationFields.nonEmpty && outputFields.nonEmpty && allowProration) {
//           val unPrunedRowSize = relation.dataSchema.fields
//             .filterNot(isInternalComputedField).map(_.dataType.defaultSize).sum
//           val prunedRowSize = if (outputFields.nonEmpty) {
//             outputFields.map(_.dataType.defaultSize).sum
//           } else {
//             math.min(LongType.defaultSize, unPrunedRowSize)
//           }
//           unPrunedRowSize.toFloat / prunedRowSize.toFloat
//         } else {
//           1.0f
//         }

//       val targetFileSizeHint = if (autoTuneOptimizedWritesEnabled) {
//         if (conf.getConf(DatabricksSQLConf.PHOTON_ENABLED)) {
//           conf.getConf(DatabricksSQLConf.AUTO_TUNE_OPTIMIZED_WRITES_TARGET_FILE_SIZE)
//         } else {
//           // Due to lack of prefetching in non-photon clusters, we are picking a smaller target size
//           // to keep the performance regression minimal compared to normal writes.
//           conf.getConf(DatabricksSQLConf.AUTO_TUNE_OPTIMIZED_WRITES_NON_PHOTON_TARGET_FILE_SIZE)
//         }
//       } else {
//         0L
//       }

//       // The logic below first determines what should be the output size of partitions and then
//       // calculates the amount of data that needs to be scanned to achieve the output size.
//       // [maxPartitionBytes] refers to the latter and will serve as bin size for the bin-packing
//       // logic that follows.

//       // prorationFactor is > 1 when the projection involves subset of the fields in the relation.
//       val totalOutputSize = math.ceil(totalBytes / prorationFactor)
//       val outputPerCore = totalOutputSize / defaultParallelism
//       // If we are scanning huge amount of data, we want partition size to be capped at
//       // defaultMaxSplitBytesConf.
//       // If the amount of data ingested is less, then each core gets the
//       // fraction of the total data being scanned. However, if there is a file size we are targeting
//       // for, we pick the target size even if we don't occupy all the cores in the cluster.
//       val maxPartitionOutputBytes = math.min(
//         defaultMaxSplitBytesConf.toDouble, math.max(outputPerCore, targetFileSizeHint.toDouble))
//       // Calculate the amount of data to be scanned to achieve the given output size
//       val maxPartitionBytes = math.min(proratedMaxPartitionBytesUpperBound,
//         math.ceil(maxPartitionOutputBytes * prorationFactor).toLong)

//       val ret = math.max(maxPartitionBytes, openCostInBytes)
//       setAutoTuneOptimizedWriteMetrics(prorationFactor, ret, defaultParallelism)
//       ret
//     }

//     /**
//      * Returns the max number of bytes per partition for the current scan file listing
//      * representation.
//      */
//     def getMaxPartitionBytes: Long = {
//       val totalBytes = calculateTotalPartitionBytes
//       val defaultParallelism =
//         relation.sparkSession.sharedState.adaptiveParallelism.defaultParallelism
//       val bytesPerCore = totalBytes / defaultParallelism
//       val parallelismFactor =
//         conf.getConf(DatabricksSQLConf.FILES_PRORATE_MAX_PARTITION_BYTES_PARALLELISM_FACTOR)
//       val proratedMaxPartitionBytesUpperBound = math.min(
//         (bytesPerCore / parallelismFactor).intValue(),
//         conf.getConf(DatabricksSQLConf.FILES_PRORATE_MAX_PARTITION_BYTES_UBOUND))

//       val defaultMaxSplitBytesConf = conf.filesMaxPartitionBytes
//       val relationFields = relation.dataSchema.fields.filterNot(isInternalComputedField)
//       val outputFields = requiredSchema.fields.filterNot(isInternalComputedField)
//       // If the initial number of splits before proration is below
//       // `parallelismFactor * defaultParallelism`, we don't prorate at all; Otherwise, we will try
//       // prorating but set the `parallelismFactor * defaultParallelism` as the lower bound for the
//       // number of splits after proration.
//       val proratedMaxPartitionBytes =
//         if (conf.getConf(DatabricksSQLConf.FILES_PRORATE_MAX_PARTITION_BYTES_ENABLED)
//             && relationFields.nonEmpty
//             && defaultMaxSplitBytesConf < proratedMaxPartitionBytesUpperBound) {
//           val relationSize = relationFields.map(_.dataType.defaultSize).sum
//           val outputSize = if (outputFields.nonEmpty) {
//             outputFields.map(_.dataType.defaultSize).sum
//           } else {
//             math.min(LongType.defaultSize, relationSize)
//           }
//           val prorated = if (relationSize == outputSize) {
//             // Avoids division by 0 error if both sizes are 0 (when only reading computed columns).
//             defaultMaxSplitBytesConf
//           } else {
//             defaultMaxSplitBytesConf * relationSize / outputSize
//           }
//           math.min(prorated, proratedMaxPartitionBytesUpperBound)
//         } else {
//           defaultMaxSplitBytesConf
//         }

//       Math.min(proratedMaxPartitionBytes, Math.max(openCostInBytes, bytesPerCore))
//     }
//   }

//   /**
//    * A file listing that represents a file list as an array of [[PartitionDirectory]]. This extends
//    * [[InnerScanFileListing]] in order to access methods for computing partition sizes and so forth.
//    * This is the default file listing class used for all table formats.
//    */
  private case class GenericScanFileListing(partitionDirectories: Array[PartitionDirectory])
    extends ScanFileListing { // Probably need to use inner Scan File Listing trait here...
    // override def fileSizeIteratorAndFileCount: (Iterator[Long], Long) = {
    //   val fileSizeIterator = partitionDirectories.iterator.flatMap(_.files.iterator.map(_.getLen))
    //   (fileSizeIterator, totalNumberOfFiles)
    // }

    override def partitionCount: Int = partitionDirectories.length

    override def totalFileSize: Long = partitionDirectories.map(_.files.map(_.getLen).sum).sum

    override def totalNumberOfFiles: Long = partitionDirectories.map(_.files.length).sum.toLong

    override def filterAndPruneFiles(
        boundPredicate: BasePredicate,
        dynamicFileFilters: Seq[Expression]): ScanFileListing = {
      val filteredPartitions = partitionDirectories.filter(p => boundPredicate.eval(p.values))
      val prunedPartitions = if (dynamicFileFilters.nonEmpty) {
        val filePruningRunner = new FilePruningRunner(dynamicFileFilters)
        filteredPartitions.map(filePruningRunner.prune)
      } else {
        filteredPartitions
      }
      GenericScanFileListing(prunedPartitions)
    }

    // override def selectedPartitionFilesAbsolutePaths: Seq[String] =
    //   partitionDirectories.flatMap(_.files.map(_.getPath.toString))

    override def toPartitionArray: Array[PartitionedFile] = {
      partitionDirectories.flatMap { p =>
        p.files.map { f =>
          PartitionedFileUtil.getPartitionedFile(f, p.values, 0, f.getLen)
          // val basePathKey = getBasePath(f.getPath).map(SparkPath.fromPath)
          // PartitionedFileUtil.getPartitionedFile(f, p.values, basePathKey)
        }
      }
    }

    override def calculateTotalPartitionBytes(openCostInBytes: Long): Long = {
      partitionDirectories.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    }

    override def filePartitionIterator: Iterator[ListingPartition] = {
      partitionDirectories.iterator.map { partitionDirectory =>
        ListingPartition(
          partitionDirectory.values,
          partitionDirectory.files.size,
          partitionDirectory.files.iterator)
      }
    }

    override def bucketsContainSingleFile: Boolean = {
      val files = partitionDirectories.flatMap(_.files)
      val bucketToFilesGrouping =
        files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
      bucketToFilesGrouping.forall(p => p._2.length <= 1)
    }
  }

//   /**
//    * Creates partitions for a non-bucketed read from either prepared partitions or
//    * dynamically selected partitions.
//    */
//   protected def createPartitionsForNonBucketedRead(): Seq[FilePartition] = {
//     relation.location match {
//       case index: TahoeFileIndexWithStaticPartitions => index.getStaticPartitions
//       case _ => createPartitionsFromSelectedPartitions(dynamicallySelectedPartitions).toSeq
//     }
//   }

//   /**
//    * Creates partitions for a non-bucketed read, given a list of selected partitions.
//    *
//    * @param selectedPartitions the partitions to read after any static partition filtering.
//    */
//     protected def createPartitionsFromSelectedPartitions(
//       selectedPartitions: ScanFileListing): ArrayBuffer[FilePartition] = FrameProfiler
//       .record("DataSourceScanExec", "createPartitionsFromSelectedPartitions") {
//     val conf = relation.sparkSession.sessionState.conf
//     // Configs for consistent splitting and locality aware partitioning
//     val consistentFileSplittingEnabled = conf.getConf(
//       DatabricksSQLConf.CONSISTENT_FILE_SPLITTING_ENABLED)

//     // Allow locality aware partitions if and only if:
//     // - consistent file splitting is enabled
//     // - parquet caching is enabled
//     // - target file size is ignored (no LSM happening)
//     val localityAwarePartitionsAllowed = consistentFileSplittingEnabled && parquetCachingAllowed &&
//       targetFileSizeOption == TargetFileSizeOption.Ignore
//     val maxNumSplitsPerChunk = conf.getConf(DatabricksSQLConf.MAX_SPLITS_PER_CHUNK)
//     val defaultMaxSplitBytesConf = conf.filesMaxPartitionBytes
//     val maxPartitionBytes = if (autoTuneOptimizedWritesEnabled) {
//       selectedPartitions.asInstanceOf[InnerScanFileListing]
//         .getMaxPartitionBytesForAutoTuneOptimizedWrites
//     } else {
//       selectedPartitions.asInstanceOf[InnerScanFileListing].getMaxPartitionBytes
//     }

//     val (maxSplitBytes, maxSplitBytesWithOvershoot) =
//       targetFileSizeOption match {
//         case TargetFileSizeOption.Ignore | TargetFileSizeOption.DoNotCombine =>
//           if (isParquetSource) {
//             if (localityAwarePartitionsAllowed) {
//               // Pin the split size to a fixed value regardless of the `proratedMaxPartitionBytes`.
//               (defaultMaxSplitBytesConf, defaultMaxSplitBytesConf)
//             } else {
//               // Make sure not to generate splits smaller than the default row group size.
//               // Otherwise the locality manager will be confused, when caching is enabled.
//               val maxSplit = math.max(defaultMaxSplitBytesConf, maxPartitionBytes)
//               (maxSplit, maxSplit)
//             }
//           } else {
//             (maxPartitionBytes, maxPartitionBytes)
//           }
//         case TargetFileSizeOption.SplitTo(targetFileSize, _) =>
//           val defaultRowGroupSize = conf.filesMaxPartitionBytes // same as parquet.block.size
//           // round up to nearest multiple of the default row group size
//           val numFullRowGroups = (targetFileSize + defaultRowGroupSize - 1) / defaultRowGroupSize
//           val splitTarget = defaultRowGroupSize * numFullRowGroups
//           // Allowing an extra half target size avoids a later compaction
//           val overshootTarget = splitTarget + targetFileSize / 2
//           (splitTarget, overshootTarget)
//       }

//     // Number of splits of default size needed to fully fill a partition rounded down.
//     val numSplitsToFillPartition = Math.max((maxPartitionBytes / defaultMaxSplitBytesConf).toInt, 1)

//     logInfo(s"Planning scan with bin packing, max split size: $maxSplitBytes bytes, " +
//       s"max partition size: $maxPartitionBytes, " +
//       s"open cost is considered as scanning $openCostInBytes bytes.")

//     // Filter files with bucket pruning if possible
//     val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
//     val shouldProcess: Path => Boolean = optionalBucketSet match {
//       case Some(bucketSet) if bucketingEnabled =>
//         // Do not prune the file if bucket file name is invalid
//         filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
//       case _ =>
//         _ => true
//     }

//     var chunkIndex = 0L
//     val splitFiles = FrameProfiler.record("DataSourceScanExec",
//     "createPartitionsFromSelectedPartitions:splitFiles") {
//     selectedPartitions.filePartitionIterator.flatMap { partition =>
//       val ListingPartition(partitionVals, _, fileStatusIterator) = partition
//       fileStatusIterator.flatMap { file =>
//         // getPath() is very expensive so we only want to call it once in this block:
//         val filePath = file.getPath
//         if (shouldProcess(filePath)) {
//           val basePath = getBasePath(filePath).map(SparkPath.fromPath)
//           val fileUri = filePath.toUri
//           // This is cheap as we construct the URI from string.
//           val fileUriString = fileUri.toString
//           val rowIndexFilterForEntireFile =
//             getRowIndexFilter(
//               fileUriString,
//               file.fileStatus)

//           if (relation.fileFormat.isSplitable(
//             relation.sparkSession, relation.options, filePath)) {
//             // For each file, use a different chunk and reset the split count per chunk.
//             // The `chunkIndex` will be ignored if: the parquet caching is OFF,
//             // or files are of different formats, or files are not splittable.
//             chunkIndex += 1
//             var splitsInChunk = 0
//             var offset = 0L
//             val fileSplits = ArrayBuffer.empty[FileSplit]
//             // We split a file into maxSplitBytes partitions.
//             // In case the tail of the file is larger than maxSplitBytes but smaller than
//             // maxSplitBytesWithOvershoot it is assigned a separate bucket.
//             while (offset < file.getLen) {
//               val remainingBytes = file.getLen - offset
//               val splitSize = if (remainingBytes > maxSplitBytesWithOvershoot) {
//                 maxSplitBytes
//               } else {
//                 // Remaining bytes fit into maxSplitBytesWithOvershoot, so put them into the
//                 // same split instead of creating a second tiny split at the end.
//                 remainingBytes
//               }

//               // If the file needs to be split below row group size using overread,
//               // this produces the correct number of non-overlapping filters for each
//               // duplicated split.
//               val rowIndexFiltersForFileSplits: Seq[Option[RowIndexFilterProvider]] =
//               targetFileSizeOption.getRowIndexFiltersForSplit(
//                 splitSize, rowIndexFilterForEntireFile)
//               assert(rowIndexFiltersForFileSplits.nonEmpty) // This would lose the split.

//               // Increment chunk index if reached maximum splits per chunk.
//               if (splitsInChunk == maxNumSplitsPerChunk) {
//                 chunkIndex += 1
//                 splitsInChunk = 0
//               }

//               val chunk = if (file.getLen == splitSize) {
//                 None
//               } else {
//                 Some(chunkIndex)
//               }

//               val hosts = getBlockHosts(file.blockLocations, offset, splitSize)
//               // Multiple rowIndexFilters must be non-overlapping and are used to extract
//               // different subsets of the rows in different tasks.
//               for (rowIndexFilter <- rowIndexFiltersForFileSplits) {
//                 fileSplits.append(FileSplit(PartitionedFile(
//                   partitionVals,
//                   SparkPath.fromUrlString(fileUriString),
//                   offset,
//                   splitSize,
//                   hosts,
//                   file.getModificationTime,
//                   basePath,
//                   rowIndexFilter,
//                   file.getLen,
//                   file.metadata
//                 ), chunk))
//                 splitsInChunk += 1
//               }
//               offset += splitSize
//             }
//             fileSplits
//           } else {
//             val hosts = getBlockHosts(file.blockLocations, 0, file.getLen)
//             Seq(FileSplit(PartitionedFile(
//               partitionVals,
//               SparkPath.fromUrlString(fileUriString),
//               0,
//               file.getLen,
//               hosts,
//               file.getModificationTime,
//               basePath,
//               rowIndexFilterForEntireFile,
//               file.getLen,
//               file.metadata
//             ), None))
//           }
//         } else {
//           Seq.empty
//         }
//       }
//     }.toSeq
//     }

//     val splitFilesOrdered = FrameProfiler.record("DataSourceScanExec",
//       "createPartitionsFromSelectedPartitions:splitFilesOrdered") {
//       if (shouldSortSourceFilesByPath) {
//         measureRuntimeMsAndRecordDriverMetric(FileSourceScanExec.FILE_PATH_SORTING_TIME_METRIC, {
//           splitFiles
//             .sortBy(file => (file.partitionedFile.urlEncodedPath, file.partitionedFile.start))
//         })
//       } else if (shouldSortSourceFilesBySize) {
//         measureRuntimeMsAndRecordDriverMetric(FileSourceScanExec.FILE_SIZE_SORTING_TIME_METRIC, {
//           splitFiles.sortBy(_.partitionedFile.length)(implicitly[Ordering[Long]].reverse)
//         })
//       } else {
//         splitFiles
//       }
//     }

//     // Create a mapping between the lists of preferred hosts and the lists of file splits.
//     // Each of the splits to be read is presents in exactly one entry.
//     // This represents where given file split is available locally, or where it should be cached.
//     // If we are in `preserveInputOrderMode`, then the preferred hosts are determined later
//     // after we group splits into partitions. In this mode, the ordering of the splits takes
//     // precedence over the preferred locality of the individual splits.
//     val preferredHostsToPartitionedFiles: Seq[(Seq[String], Seq[PartitionedFile])] = FrameProfiler
//       .record("DataSourceScanExec",
//         "createPartitionsFromSelectedPartitions:preferredHostsToPartitionedFiles") {
//       if (parquetCachingAllowed) {
//         // When the Parquet page cache is to be used, ParquetLocalityManager assigns the preferred
//         // hosts to splits.
//         // With preserveInputOrderMode we do not want to bucket by host by
//         // bucket sequentially in the order the file already are. This will be done implicitly in
//         // closePartition. In this case we don't assign a preferred host to the files,
//         // but the preferred hosts for the sequentially bucketed files will be determined by
//         // `FilePartition.preferredLocations`.
//         val beforeBucketByHost = System.nanoTime()
//         val bucketed = if (plv2Enabled) {
//           // TODO: handle `cacheSelectNumReplicas`
//           PreferredLocationsV2
//             .getImpl(relation.sparkSession.sparkContext)
//             .bucketByHost(splitFilesOrdered)
//             .mapValues(_.map(_.partitionedFile))
//         } else if (consistentFileSplittingEnabled) {
//           ParquetLocalityManager.bucketByHostWithSplitLocalityAwareness(
//             relation.sparkSession.sparkContext,
//             splitFilesOrdered,
//             cacheSelectNumReplicas)
//         } else {
//           ParquetLocalityManager.bucketByHost(
//             relation.sparkSession.sparkContext,
//             splitFilesOrdered.map(_.partitionedFile),
//             cacheSelectNumReplicas)
//         }
//         if (!plv2Enabled) {
//           driverMetrics("cacheLocalityMgrTimeMs").set(
//             NANOSECONDS.toMillis(System.nanoTime() - beforeBucketByHost))
//           driverMetrics("cacheLocalityMgrDiskUsage").set(ParquetLocalityManager.getDiskUsage())
//         }

//         if (bucketed.nonEmpty) {
//           val splitSizesByHost = bucketed.values.map(s => s.map(_.fileSize).sum)
//           driverMetrics(FileSourceScanExec.AVG_TOTAL_SPLITS_SIZE_PER_HOST).set(
//             splitSizesByHost.sum / splitSizesByHost.size)
//           driverMetrics(FileSourceScanExec.RELATIVE_TOTAL_SPLITS_SIZE_PER_HOST).set(
//             ((splitSizesByHost.max - splitSizesByHost.min) * 100D /
//               splitSizesByHost.min).toLong)
//         }

//         bucketed.toSeq.map {
//           case (null, splits) => Tuple2(null, splits)
//           case (hostname, splits) => Tuple2(Seq(hostname), splits)
//         }
//       } else {
//         // Otherwise, no preference is set.
//         val Null: Seq[String] = null
//         Seq(Null -> splitFilesOrdered.map(_.partitionedFile))
//       }
//     }

//     val partitions = new ArrayBuffer[FilePartition]

//     /** Close the current partition and move to the next. */
//     def closePartition(preferredHosts: Seq[String], currentFiles: Seq[PartitionedFile]): Unit = {
//       if (currentFiles.nonEmpty) {
//         // Copy to a new Array.
//         val newPartition = FilePartition(
//           partitions.size,
//           currentFiles.toArray,
//           preferredHosts,
//           plv2Enabled = plv2Enabled && useParquetCaching
//         )
//         partitions += newPartition
//       }
//     }

//     /**
//      * Creates partitions using knowing that the splits are ordered DESC by length.
//      * Greedily fits splits into a partition until the `maxPartitionBytes` is reached.
//      */
//     def createPartitions(
//         hostsToPartitionedFiles: Seq[(Seq[String], Seq[PartitionedFile])]): Unit = {
//       var currentFile: Option[PartitionedFile] = None
//       for ((preferredHosts, filesInBucket) <- hostsToPartitionedFiles) {
//         var currentSize = 0L
//         val currentFiles = new ArrayBuffer[PartitionedFile]
//         for (file <- filesInBucket) {
//           // close partitions at file boundaries, if requested
//           val fileMismatch = !targetFileSizeOption.mayCombineFiles &&
//             currentFile.isDefined &&
//             (currentFile.get.filePath != file.filePath ||
//               // Different filter should also be treated as a different file,
//               // so that they end up in different output files.
//               currentFile.get.getRowIndexFilter != file.getRowIndexFilter)
//           val fileDoesNotFit = currentSize + file.length > maxPartitionBytes
//           if (fileMismatch || fileDoesNotFit) {
//             closePartition(preferredHosts, currentFiles.toSeq)
//             currentFiles.clear()
//             currentSize = 0
//           }
//           currentSize += file.length + openCostInBytes
//           currentFiles += file
//           currentFile = Some(file)
//         }
//         closePartition(preferredHosts, currentFiles.toSeq)
//         currentFiles.clear()
//       }
//     }

//     /**
//      * The treemap uses a queue for values.
//      * If the key already exists, add the given `entry` value to the queue.
//      */
//     def addSplitPackToTreeMap(
//         tm: TreeMap[Long, Queue[Seq[PartitionedFile]]],
//         entry: (Long, Seq[PartitionedFile])): Unit = {
//       val (key, value) = entry
//       val packValue = tm.get(key)
//       if (packValue != null) {
//         packValue.enqueue(value)
//       } else {
//         val queue = new Queue[Seq[PartitionedFile]]
//         queue.enqueue(value)
//         tm.put(key, queue)
//       }
//     }

//     /**
//      * Dequeues and returns a split pack from the queue associated with the given `key`.
//      * If the queue is empty, removes the entry from the tree.
//      * Expected to be called with a valid key.
//      */
//     def getAndRemoveSplitPackFromTreeMap(
//         tm: java.util.TreeMap[Long, Queue[Seq[PartitionedFile]]],
//         key: Long): Seq[PartitionedFile] = {
//       val queue = tm.get(key)
//       val splitPack = queue.dequeue()
//       if (queue.isEmpty) {
//         tm.remove(key)
//       }
//       splitPack
//     }

//     /**
//      * Merges adjacent splits into a single split.
//      * Adds merged splits to partition.
//      * Returns the size of splits added to partition.
//      */
//     def addMergedSplitsToPartition(
//         startSplitIndex: Int,
//         lastSplitIndex: Int,
//         splitPack: Seq[PartitionedFile],
//         currentPartition: ArrayBuffer[PartitionedFile],
//         currentPartitionSize: Long = 0L): Long = {
//       currentPartition += splitPack(startSplitIndex)
//       var partitionSize = currentPartition.last.length + openCostInBytes + currentPartitionSize
//       var index = startSplitIndex + 1
//       while (index < lastSplitIndex) {
//         val nextSplit = splitPack(index)
//         if (currentPartition.last.start + currentPartition.last.length == nextSplit.start) {
//           // Split is mergeable
//           val mergedLength = currentPartition.last.length + nextSplit.length
//           val mergedSplit = currentPartition.last.copy(length = mergedLength)
//           currentPartition.update(currentPartition.size - 1, mergedSplit)
//           partitionSize += nextSplit.length
//         } else {
//           currentPartition += nextSplit
//           // Each new split added should also add an `openCostInBytes`
//           // to avoid having very many small files in a single partition.
//           partitionSize += nextSplit.length + openCostInBytes
//         }
//         index += 1
//       }
//       partitionSize
//     }

//     /**
//      * Creates partitions filled with splits from the same file where possible.
//      * If there is still space in the partition after using all of the remaining
//      * splits from a file, tries to fill with splits from other files.
//      */
//     def createPartitionsWithGroupedSplits(
//        hostsToPartitionedFiles: Seq[(Seq[String], Seq[PartitionedFile])]): Unit = {

//       hostsToPartitionedFiles.foreach { case (preferredHosts, filesInBucket) =>
//         // Group splits by file name
//         // A split pack contains all the splits from the same file for the current `prefferedHosts`.
//         val splitPacks = filesInBucket.groupBy(_.filePath).mapValues {
//           splits =>
//             // Sort splits by starting offset
//             (splits.map(_.length).sum, splits.sortBy(_.start))
//         }

//         val splitPacksToPartition = new java.util.TreeMap[Long, Queue[Seq[PartitionedFile]]]
//         splitPacks.foreach(pack => addSplitPackToTreeMap(splitPacksToPartition, pack._2))

//         val currentPartition = new ArrayBuffer[PartitionedFile]

//         // Fill partitions from the largest packs of splits, until the partition is full.
//         while (splitPacksToPartition.size() > 0) {
//           // Find the largest pack
//           val largestKey = splitPacksToPartition.lastKey()
//           val largestSplitPack = getAndRemoveSplitPackFromTreeMap(splitPacksToPartition, largestKey)

//           if (largestSplitPack.size >= numSplitsToFillPartition) {
//             val numRemainingSplits = largestSplitPack.size % numSplitsToFillPartition
//             // Make partitions with splits from a single file.
//             val splitsRange = 0.until(largestSplitPack.size - numRemainingSplits)
//               .by(numSplitsToFillPartition)
//             for (startIndex <- splitsRange) {
//               addMergedSplitsToPartition(startIndex, startIndex + numSplitsToFillPartition,
//                 largestSplitPack, currentPartition)
//               closePartition(preferredHosts, currentPartition.toSeq)
//               currentPartition.clear()
//             }
//             // Put back into tree the rest of splits which can't fill a partition by themselves.
//             if (numRemainingSplits > 0) {
//               val remainingSplits = largestSplitPack.takeRight(numRemainingSplits)
//               addSplitPackToTreeMap(splitPacksToPartition,
//                 (remainingSplits.map(_.length).sum, remainingSplits))
//             }
//           } else {
//             // Make partitions with splits from multiple files.

//             // Merge splits from the pack and add them to partition.
//             var currentPartitionSize = addMergedSplitsToPartition(startSplitIndex = 0,
//               largestSplitPack.size, largestSplitPack, currentPartition)
//             // Try to fill remaining space in the partition with splits from other packs.
//             var fittingPacks = splitPacksToPartition.floorEntry(
//               maxPartitionBytes - currentPartitionSize)

//             while (fittingPacks != null) {
//               val fittingKey = fittingPacks.getKey
//               val splitPack = getAndRemoveSplitPackFromTreeMap(splitPacksToPartition, fittingKey)
//               currentPartitionSize = addMergedSplitsToPartition(startSplitIndex = 0,
//                 splitPack.size, splitPack, currentPartition, currentPartitionSize)

//               fittingPacks = splitPacksToPartition.floorEntry(
//                 maxPartitionBytes - currentPartitionSize)
//             }
//             closePartition(preferredHosts, currentPartition.toSeq)
//             currentPartition.clear()
//             currentPartitionSize = 0L
//           }
//         }
//       }
//     }

//     // Create the FilePartitions so that the splits with the same preferred hosts are grouped
//     // together, and splits with different preferred hosts are separated. This will allow the
//     // scheduler to place the tasks on hosts where the data is locally available/should be cached.
//     if (localityAwarePartitionsAllowed && numSplitsToFillPartition > 1) {
//       createPartitionsWithGroupedSplits(preferredHostsToPartitionedFiles)
//     } else {
//       createPartitions(preferredHostsToPartitionedFiles)
//     }

//     if (computeEstRepeatedReads) {
//       RepeatedReadsEstimator.setExpectedRepeatedReadsRatios(partitions.toSeq)
//     }

//     partitions
//   }

//   private def measureRuntimeMsAndRecordDriverMetric[T](metric: String, f: => T): T = {
//     val startTime = System.nanoTime()
//     val ret = f
//     val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
//     driverMetrics(metric).set(timeTakenMs)
//     ret
//   }

  // protected def getBasePath(filePath: Path): Option[Path] =
  //   if (useParquetCaching &&
  //       conf.getConf(DatabricksSQLConf.IO_CACHE_LOCALITY_MANAGER_PATH_PREFIXES)) {
  //     relation.location match {
  //       case t: TahoeFileIndex =>
  //         t.getBasePath(filePath)
  //       case p: PartitioningAwareFileIndex =>
  //         p.getBasePath(filePath)
  //       case c: CatalogFileIndex =>
  //         c.getBasePath(filePath)
  //       case _ =>
  //         None
  //     }
  //   } else {
  //     None
  //   }


//   // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
//   // pair that represents a segment of the same file, find out the block that contains the largest
//   // fraction the segment, and returns location hosts of that block. If no such block can be found,
//   // returns an empty array.
//   private def getBlockHosts(
//       blockLocations: Array[SerializableBlockLocation],
//       offset: Long,
//       length: Long): Seq[String] = {
//     if (blockLocations.length == 0) {
//       Nil
//     } else {
//       val candidates = blockLocations.map {
//         // The fragment starts from a position within this block
//         case b if b.offset <= offset && offset < b.offset + b.length =>
//           b.hosts -> Math.min(b.offset + b.length - offset, length)

//         // The fragment ends at a position within this block
//         case b if offset <= b.offset && offset + length < b.length =>
//           b.hosts -> Math.min(offset + length - b.offset, length)

//         // The fragment fully contains this block
//         case b if offset <= b.offset && b.offset + b.length <= offset + length =>
//           b.hosts -> b.length

//         // The fragment doesn't intersect with this block
//         case b =>
//           b.hosts -> 0L
//       }.filter { case (hosts, size) =>
//         size > 0L
//       }

//       if (candidates.isEmpty) {
//         Nil
//       } else {
//         val (hosts, _) = candidates.maxBy { case (_, size) => size }
//         hosts
//       }
//     }
//   }

//   override def computeStats(): Statistics = {
//     // if partition filters are pushed down, they are removed from the query plan
//     // normal filters, even if pushed down, remain in the query plan
//     // therefore, we consider partition filters here, but we don't consider
//     // normal filters. if we did then we would be doing filter estimation twice
//     // see [[FileSourceStrategy]]
//     // todo: check FileSourceStrategy in more detail
//     val withFilterIfNeeded: LogicalPlan = partitionFilters.reduceOption(And).map { cond =>
//       logical.Filter(cond, logicalRelation)
//     }.getOrElse(logicalRelation)
//     withFilterIfNeeded.stats
//   }

//   private lazy val isCloudFilesSource: Boolean =
//   // We don't have access to the class here, so just check the name
//     relation.location.getClass.getSimpleName == "CloudFilesSourceFileIndex"

//   protected lazy val fileNotFoundHint: String => String = {
//     // Build the hint generator and store in a variable so that it is captured in the
//     // returned lambda (instead of all of the context and member variables required to build
//     // the hint generator).
//     val hintGenerator = buildFileNotFoundHintGenerator
//     path => hintGenerator.generateAndLogErrorMsg(path)
//   }

//   protected def buildFileNotFoundHintGenerator: FileNotFoundHintGenerator = {
//     if (isDeltaSource) {
//       val isReadingCDC =
//         relation.dataSchema.fields.map(_.name).contains(CDCReader.CDC_TYPE_COLUMN_NAME)
//       // We can't generate the link inside `deltaFileNotFoundHint` like normal because it requires
//       // passing in the spark conf which would then need to be serialized and throws an error in the
//       // process
//       val faqPath = DeltaErrors.generateDocsLink(
//         sparkContext.getConf,
//         DeltaErrors.faqRelativePath,
//         skipValidation = true)
//       val deltaRootPath = relation.location.rootPaths.head.toString
//       new DeltaFileNotFoundHintGenerator(
//         deltaRootPath = deltaRootPath, isReadingCDC = isReadingCDC, faqPath = faqPath)
//     } else if (isCloudFilesSource) {
//       new CloudFileNotFoundHintGenerator
//     } else {
//       new DefaultFileNotFoundHintGenerator
//     }
//   }

//   protected def debugWriter: Option[BadFilesWriter] = {
//     val options = CaseInsensitiveMap(relation.options)
//     if (options.get("badRecordsPath").isDefined) {
//       val loc = options.get("badRecordsPath").get
//       Some(new BadFilesWriterJson(loc, new SerializableConfiguration(hadoopConf)))
//     } else {
//       None
//     }
//   }

//   /**
//    * Use this function to record how long this scan waited for build side to materialize.
//    * @param durationNs duration for which scan waited in nanoseconds.
//    */
//   def setWaitTimeForBuildSideToMateralize(durationNs: Long): Unit =
//     if (driverMetrics.contains(
//       FileSourceScanExec.STAGE_WAIT_DURATION_FOR_BUILD_SIDE_TO_MATERIALIZE)) {
//       driverMetrics(FileSourceScanExec.STAGE_WAIT_DURATION_FOR_BUILD_SIDE_TO_MATERIALIZE)
//         .set(durationNs)
//     }
// }

// /**
//  * Base trait for Spark file source scans.
//  */
// trait FileSourceScanLike extends DataSourceScanExec with SparkOrAetherFileSourceScanLike {
//   /**
//    * Create a new FileSourceScanLike, reusing states from an input FileSourceScanLike.
//    */
//   def copyReusableStatesFrom(reusableScan: FileSourceScanLike): FileSourceScanLike = {
//     // The new scan node may have additional dynamic pruning filters because the Spark planner
//     // does not remove them from scan nodes for a broadcast hash join (they are removed if
//     // it is a shuffle join). Currently if not removed, the subquery can be executed while the
//     // filter will not be used as DFP and will not be effective in pushedDownFilters.
//     val newDataFilters = removeNewDynamicPruningFilters(this.dataFilters,
//       reusableScan.dataFilters)
//     val newPartitionFilters = if (conf.getConf(
//         DatabricksSQLConf.AdaptiveExecution.DYNAMIC_PARTITION_PRUNING_IN_STAGE_CANCELLATION)) {
//       this.partitionFilters
//     } else {
//       removeNewDynamicPruningFilters(this.partitionFilters, reusableScan.partitionFilters)
//     }
//     val newNode = reuseFileListingResultsWithNewFilters(
//       sourceNode = reusableScan,
//       dataFilters = newDataFilters,
//       partitionFilters = newPartitionFilters)
//     newNode.driverMetrics ++= reusableScan.driverMetrics
//     newNode
//   }

//   /**
//    * Copy the current node with new dataFilters, partitionFilters, and reusedFileListingCount
//    * A side effect of this copying is that we set `preserveInputOrderMode` to false if this flag of
//    * the sourceNode is false when copying.
//    */
//   protected def reuseFileListingResultsWithNewFilters(
//       sourceNode: FileSourceScanLike,
//       dataFilters: Seq[Expression],
//       partitionFilters: Seq[Expression]): FileSourceScanLike
// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)
}

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan, including data attributes and partition attributes.
 * @param requiredSchema Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param optionalBucketSet Bucket ids for bucket pruning.
 * @param optionalNumCoalescedBuckets Number of coalesced buckets.
 * @param dataFilters Filters on non-partition columns.
 * @param tableIdentifier Identifier for the table in the metastore.
 * @param disableBucketedScan Disable bucketed scan based on physical query plan, see rule
 *                            [[DisableUnnecessaryBucketedScan]] for details.
 */
case class FileSourceScanExec(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanLike {

  // Note that some vals referring the file-based relation are lazy intentionally
  // so that this plan can be canonicalized on executor side too. See SPARK-23731.
  override lazy val supportsColumnar: Boolean = {
    val conf = relation.sparkSession.sessionState.conf
    // Only output columnar if there is WSCG to read it.
    val requiredWholeStageCodegenSettings =
      conf.wholeStageEnabled && !WholeStageCodegenExec.isTooManyFields(conf, schema)
    requiredWholeStageCodegenSettings &&
      relation.fileFormat.supportBatch(relation.sparkSession, schema)
  }

  private lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[ParquetSource]) {
      conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions)
    } else {
      createReadRDD(readFile, dynamicallySelectedPartitions)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    if (needsUnsafeRowConversion) {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        val toUnsafe = UnsafeProjection.create(schema)
        toUnsafe.initialize(index)
        iter.map { row =>
          numOutputRows += 1
          toUnsafe(row)
        }
      }
    } else {
      inputRDD.mapPartitionsInternal { iter =>
        iter.map { row =>
          numOutputRows += 1
          row
        }
      }
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override val nodeNamePrefix: String = "File"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
// <<<<<<< HEAD
      selectedPartitions: ScanFileListing): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    // val filesGroupedToBuckets =
    //   selectedPartitions.flatMap { p =>
    //     p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, p.values, 0, f.getLen))
    //   }.groupBy { f =>
    //     BucketingUtils
    //       .getBucketId(f.toPath.getName)
    //       .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
    //   }
    val partitionArray = selectedPartitions.toPartitionArray
    val filesGroupedToBuckets = partitionArray.groupBy { f =>
      BucketingUtils
        .getBucketId(f.toPath.getName)
        .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
    }
// =======
    //   selectedPartitions: ScanFileListing,
    //   asyncIOSafe: Boolean): RDD[InternalRow] = {
    // logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    // val bucketIndex = GeneratedColumn.extractDeltaBucketIndex(relation, bucketSpec)
    // val partitionArray = selectedPartitions.toPartitionArray
    // val filesGroupedToBuckets = partitionArray.groupBy { f =>
    //   bucketIndex.map(index => f.partitionValues.getInt(index)).orElse(
    //     BucketingUtils.getBucketId(f.toPath.getName))
    //     .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
    // }
// >>>>>>> 690ca6ea2b7 ([SC-147445] Refactor file listing with ScanFileListing interface)

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val partitionedFiles = coalescedBuckets.get(bucketId).map {
          _.values.flatten.toArray
        }.getOrElse(Array.empty)
        FilePartition(bucketId, partitionedFiles)
      }
    }.getOrElse {
      Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
        FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }
    }

    new FileScanRDD(relation.sparkSession, readFile, filePartitions,
      new StructType(requiredSchema.fields ++ relation.partitionSchema.fields),
      fileConstantMetadataColumns, relation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(relation.options)))
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   */
  private def createReadRDD(
      readFile: PartitionedFile => Iterator[InternalRow],
      selectedPartitions: ScanFileListing): RDD[InternalRow] = {
    val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
//    val selectedPartitionDirectories = selectedPartitions.asInstanceOf[GenericScanFileListing]
//      .partitionDirectories
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    // Filter files with bucket pruning if possible
    val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    // val splitFiles = selectedPartitions.flatMap { partition =>
    //   partition.files.flatMap { file =>
    //     if (shouldProcess(file.getPath)) {
    //       val isSplitable = relation.fileFormat.isSplitable(
    //           relation.sparkSession, relation.options, file.getPath)
    //       PartitionedFileUtil.splitFiles(
    //         file = file,
    //         isSplitable = isSplitable,
    //         maxSplitBytes = maxSplitBytes,
    //         partitionValues = partition.values
    //       )
    //     } else {
    //       Seq.empty
    //     }
    //   }
    // }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    val splitFiles = selectedPartitions.filePartitionIterator.flatMap { partition =>
      val ListingPartition(partitionVals, _, fileStatusIterator) = partition
      fileStatusIterator.flatMap { file =>
        if (shouldProcess(file.getPath)) {
          val isSplitable = relation.fileFormat.isSplitable(
              relation.sparkSession, relation.options, file.getPath)
          PartitionedFileUtil.splitFiles(
            file = file,
            isSplitable = isSplitable,
            maxSplitBytes = maxSplitBytes,
            partitionValues = partitionVals
          )
        } else {
          Seq.empty
        }
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = FilePartition
      .getFilePartitions(relation.sparkSession, splitFiles.toImmutableArraySeq, maxSplitBytes)

    new FileScanRDD(relation.sparkSession, readFile, partitions,
      new StructType(requiredSchema.fields ++ relation.partitionSchema.fields),
      fileConstantMetadataColumns, relation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(relation.options)))
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  private def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  override def doCanonicalize(): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan)
  }
}
