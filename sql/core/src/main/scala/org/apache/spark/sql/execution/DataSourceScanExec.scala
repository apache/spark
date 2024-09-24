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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.LogKeys.{COUNT, MAX_SPLIT_BYTES, OPEN_COST_IN_BYTES}
import org.apache.spark.internal.MDC
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
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.datasources.v2.PushedDownOperators
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, VariantConstructionMetrics}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{StructType, VariantType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.types.variant.VariantMetrics
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

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
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
      val returnedFiles = selectedPartitions.filterAndPruneFiles(
          boundPredicate, dynamicDataFilters)
      setFilesNumAndSizeMetric(returnedFiles, false)
      val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
      driverMetrics("pruningTime").set(timeTakenMs)
      returnedFiles
    } else {
      selectedPartitions
    }
  }

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

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  protected lazy val staticMetrics = if (partitionFilters.exists(isDynamicPruningFilter)) {
    Map("staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
      "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read"))
  } else {
    Map.empty[String, SQLMetric]
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(partitions: ScanFileListing, static: Boolean): Unit = {
    val filesNum = partitions.totalNumberOfFiles
    val filesSize = partitions.totalFileSize
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
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
  } ++ driverMetrics

  /**
   * A file listing that represents a file list as an array of [[PartitionDirectory]]. This extends
   * [[ScanFileListing]] in order to access methods for computing partition sizes and so forth.
   * This is the default file listing class used for all table formats.
   */
  private case class GenericScanFileListing(partitionDirectories: Array[PartitionDirectory])
    extends ScanFileListing {

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

    override def toPartitionArray: Array[PartitionedFile] = {
      partitionDirectories.flatMap { p =>
        p.files.map { f => PartitionedFileUtil.getPartitionedFile(f, p.values, 0, f.getLen) }
      }
    }

    override def calculateTotalPartitionBytes: Long = {
      val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
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

  /** SQL metrics generated for JSON scans involving variants. */
  protected lazy val variantBuilderMetrics: Map[String, SQLMetric] =
    VariantConstructionMetrics.createSQLMetrics(sparkContext)

  /**
   * Only report variant metrics if the data source file format is JSON and the schema
   * contains a Variant
   */
  override lazy val metrics: Map[String, SQLMetric] = super.metrics ++ {
    if (relation.fileFormat.isInstanceOf[JsonFileFormat] &&
      requiredSchema.existsRecursively(_.isInstanceOf[VariantType])) {
      variantBuilderMetrics
    } else Map.empty[String, SQLMetric]
  }

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

  val topLevelVariantMetrics: VariantMetrics = new VariantMetrics()
  val nestedVariantMetrics: VariantMetrics = new VariantMetrics()

  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] = {
      val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
      relation.fileFormat match {
        case f: JsonFileFormat =>
          f.buildReaderWithPartitionValuesAndVariantMetrics(
            sparkSession = relation.sparkSession,
            dataSchema = relation.dataSchema,
            partitionSchema = relation.partitionSchema,
            requiredSchema = requiredSchema,
            filters = pushedDownFilters,
            options = options,
            hadoopConf = hadoopConf,
            topLevelVariantMetrics,
            nestedVariantMetrics
          )
        case _ =>
          relation.fileFormat.buildReaderWithPartitionValues(
            sparkSession = relation.sparkSession,
            dataSchema = relation.dataSchema,
            partitionSchema = relation.partitionSchema,
            requiredSchema = requiredSchema,
            filters = pushedDownFilters,
            options = options,
            hadoopConf = hadoopConf
          )
      }
    }

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
      selectedPartitions: ScanFileListing): RDD[InternalRow] = {
    logInfo(log"Planning with ${MDC(COUNT, bucketSpec.numBuckets)} buckets")
    val partitionArray = selectedPartitions.toPartitionArray
    val filesGroupedToBuckets = partitionArray.groupBy { f =>
      BucketingUtils
        .getBucketId(f.toPath.getName)
        .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
    }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(log"Coalescing to ${MDC(COUNT, numCoalescedBuckets)} buckets")
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
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    logInfo(log"Planning scan with bin packing, max size: ${MDC(MAX_SPLIT_BYTES, maxSplitBytes)} " +
      log"bytes, open cost is considered as scanning ${MDC(OPEN_COST_IN_BYTES, openCostInBytes)} " +
      log"bytes.")

    // Filter files with bucket pruning if possible
    val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

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
      new FileSourceOptions(CaseInsensitiveMap(relation.options)),
      Some(new FileScanMetrics(Some(topLevelVariantMetrics), Some(nestedVariantMetrics))),
      Some(variantBuilderMetrics))
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
