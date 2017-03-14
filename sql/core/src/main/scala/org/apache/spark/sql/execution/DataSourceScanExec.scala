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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

trait DataSourceScanExec extends LeafExecNode with CodegenSupport {
  val relation: BaseRelation
  val metastoreTableIdentifier: Option[TableIdentifier]

  override val nodeName: String = {
    s"Scan $relation ${metastoreTableIdentifier.map(_.unquotedString).getOrElse("")}"
  }
}

/** Physical plan node for scanning data from a relation. */
case class RowDataSourceScanExec(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    override val outputPartitioning: Partitioning,
    override val metadata: Map[String, String],
    override val metastoreTableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec {

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  val outputUnsafeRows = relation match {
    case r: HadoopFsRelation if r.fileFormat.isInstanceOf[ParquetSource] =>
      !SparkSession.getActiveSession.get.sessionState.conf.getConf(
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED)
    case _: HadoopFsRelation => true
    case _ => false
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val unsafeRow = if (outputUnsafeRows) {
      rdd
    } else {
      rdd.mapPartitionsWithIndexInternal { (index, iter) =>
        val proj = UnsafeProjection.create(schema)
        proj.initialize(index)
        iter.map(proj)
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    unsafeRow.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield {
      key + ": " + StringUtils.abbreviate(value, 100)
    }

    s"$nodeName${Utils.truncatedString(output, "[", ",", "]")}" +
      s"${Utils.truncatedString(metadataEntries, " ", ", ", "")}"
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      new BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = if (outputUnsafeRows) row else null
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  // Ignore rdd when checking results
  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: RowDataSourceScanExec => relation == other.relation && metadata == other.metadata
    case _ => false
  }
}

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan.
 * @param outputSchema Output schema of the scan.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param dataFilters Data source filters to use for filtering data within partitions.
 * @param metastoreTableIdentifier identifier for the table in the metastore.
 */
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    outputSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Filter],
    override val metastoreTableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with ColumnarBatchScan  {

  val supportsBatch: Boolean = relation.fileFormat.supportBatch(
    relation.sparkSession, StructType.fromAttributes(output))

  val needsUnsafeRowConversion: Boolean = if (relation.fileFormat.isInstanceOf[ParquetSource]) {
    SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
  } else {
    false
  }

  @transient private lazy val selectedPartitions = relation.location.listFiles(partitionFilters)

  override val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    val bucketSpec = if (relation.sparkSession.sessionState.conf.bucketingEnabled) {
      relation.bucketSpec
    } else {
      None
    }
    bucketSpec match {
      case Some(spec) =>
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
        // eg.
        // Assume (col0, col2, col3) are the columns read from the table
        // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
        // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
        // above

        def toAttribute(colName: String): Option[Attribute] =
          output.find(_.name == colName)

        val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
        if (bucketColumns.size == spec.bucketColumnNames.size) {
          val partitioning = HashPartitioning(bucketColumns, spec.numBuckets)
          val sortColumns =
            spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)

          val sortOrder = if (sortColumns.nonEmpty) {
            // In case of bucketing, its possible to have multiple files belonging to the
            // same bucket in a given relation. Each of these files are locally sorted
            // but those files combined together are not globally sorted. Given that,
            // the RDD partition will not be sorted even if the relation has sort columns set
            // Current solution is to check if all the buckets have a single file in it

            val files = selectedPartitions.flatMap(partition => partition.files)
            val bucketToFilesGrouping =
              files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
            val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

            if (singleFilePartitions) {
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
      case _ =>
        (UnknownPartitioning(0), Nil)
    }
  }

  // These metadata values make scan plans uniquely identifiable for equality checking.
  override val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName + seqToString(location.rootPaths)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> outputSchema.catalogString,
        "Batched" -> supportsBatch.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)
    val withOptPartitionCount =
      relation.partitionSchemaOption.map { _ =>
        metadata + ("PartitionCount" -> selectedPartitions.size.toString)
      } getOrElse {
        metadata
      }
    withOptPartitionCount
  }

  private lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = outputSchema,
        filters = dataFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    relation.bucketSpec match {
      case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
        createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
      case _ =>
        createNonBucketedReadRDD(readFile, selectedPartitions, relation)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      // in the case of fallback, this batched scan should never fail because of:
      // 1) only primitive types are supported
      // 2) the number of columns should be smaller than spark.sql.codegen.maxFields
      WholeStageCodegenExec(this).execute()
    } else {
      val unsafeRows = {
        val scan = inputRDD
        if (needsUnsafeRowConversion) {
          scan.mapPartitionsWithIndexInternal { (index, iter) =>
            val proj = UnsafeProjection.create(schema)
            proj.initialize(index)
            iter.map(proj)
          }
        } else {
          scan
        }
      }
      val numOutputRows = longMetric("numOutputRows")
      unsafeRows.map { r =>
        numOutputRows += 1
        r
      }
    }
  }

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield {
      key + ": " + StringUtils.abbreviate(value, 100)
    }
    val metadataStr = Utils.truncatedString(metadataEntries, " ", ", ", "")
    s"File$nodeName${Utils.truncatedString(output, "[", ",", "]")}$metadataStr"
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    if (supportsBatch) {
      return super.doProduce(ctx)
    }
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      new BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = if (needsUnsafeRowConversion) null else row
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val bucketed =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          val hosts = DefaultFilePartitionStrategy.getBlockHosts(
            DefaultFilePartitionStrategy.getBlockLocations(f), 0, f.getLen)
          PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
      FilePartition(bucketId, bucketed.getOrElse(bucketId, Nil))
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {

    val partitionStrategy =
      Utils
        .classForName(fsRelation.sparkSession.sessionState.conf.filesPartitionStrategy)
        .newInstance().asInstanceOf[FilePartitionStrategy]

    val input: Seq[(InternalRow, FileStatus)] = selectedPartitions.flatMap { partitionDirectory =>
      partitionDirectory.files.map(file => (partitionDirectory.values, file))
    }

    val partitions: Seq[FilePartition] =
      partitionStrategy
        .partition(fsRelation.sparkSession, fsRelation.fileFormat, fsRelation.options, input)
        .zipWithIndex
        .map { case (p: Seq[PartitionedFile], index) => FilePartition(index, p) }

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: FileSourceScanExec =>
      val thisPredicates = partitionFilters.map(cleanExpression)
      val otherPredicates = other.partitionFilters.map(cleanExpression)
      val result = relation == other.relation && metadata == other.metadata &&
        thisPredicates.length == otherPredicates.length &&
        thisPredicates.zip(otherPredicates).forall(p => p._1.semanticEquals(p._2))
      result
    case _ => false
  }
}
