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

package org.apache.spark.sql.execution.datasources

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{NUM_PRUNED, POST_SCAN_FILTERS, PUSHED_FILTERS, TOTAL}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.{PLAN_EXPRESSION, SCALAR_SUBQUERY}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.types.{DoubleType, FloatType, StructType}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.BitSet

/**
 * A strategy for planning scans over collections of files that might be partitioned or bucketed
 * by user specified columns.
 *
 * At a high level planning occurs in several phases:
 *  - Split filters by when they need to be evaluated.
 *  - Prune the schema of the data requested based on any projections present. Today this pruning
 *    is only done on top level columns, but formats should support pruning of nested columns as
 *    well.
 *  - Construct a reader function by passing filters and the schema into the FileFormat.
 *  - Using a partition pruning predicates, enumerate the list of files that should be read.
 *  - Split the files into tasks and construct a FileScanRDD.
 *  - Add any projection or filters that must be evaluated after the scan.
 *
 * Files are assigned into tasks using the following algorithm:
 *  - If the table is bucketed, group files by bucket id into the correct number of partitions.
 *  - If the table is not bucketed or bucketing is turned off:
 *   - If any file is larger than the threshold, split it into pieces based on that threshold
 *   - Sort the files by decreasing file size.
 *   - Assign the ordered files to buckets using the following algorithm.  If the current partition
 *     is under the threshold with the addition of the next file, add it.  If not, open a new bucket
 *     and add it.  Proceed to the next file.
 */
object FileSourceStrategy extends Strategy with PredicateHelper with Logging {

  // should prune buckets iff num buckets is greater than 1 and there is only one bucket column
  private def shouldPruneBuckets(bucketSpec: Option[BucketSpec]): Boolean = {
    bucketSpec match {
      case Some(spec) => spec.bucketColumnNames.length == 1 && spec.numBuckets > 1
      case None => false
    }
  }

  private def getExpressionBuckets(
      expr: Expression,
      bucketColumnName: String,
      numBuckets: Int): BitSet = {

    def getBucketNumber(attr: Attribute, v: Any): Int = {
      BucketingUtils.getBucketIdFromValue(attr, numBuckets, v)
    }

    def getBucketSetFromIterable(attr: Attribute, iter: Iterable[Any]): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      iter
        .map(v => getBucketNumber(attr, v))
        .foreach(bucketNum => matchedBuckets.set(bucketNum))
      matchedBuckets
    }

    def getBucketSetFromValue(attr: Attribute, v: Any): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      matchedBuckets.set(getBucketNumber(attr, v))
      matchedBuckets
    }

    expr match {
      case expressions.Equality(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, v)
      case expressions.In(a: Attribute, list)
        if list.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
        getBucketSetFromIterable(a, list.map(e => e.eval(EmptyRow)))
      case expressions.InSet(a: Attribute, hset) if a.name == bucketColumnName =>
        getBucketSetFromIterable(a, hset)
      case expressions.IsNull(a: Attribute) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, null)
      case expressions.IsNaN(a: Attribute)
        if a.name == bucketColumnName && a.dataType == FloatType =>
        getBucketSetFromValue(a, Float.NaN)
      case expressions.IsNaN(a: Attribute)
        if a.name == bucketColumnName && a.dataType == DoubleType =>
        getBucketSetFromValue(a, Double.NaN)
      case expressions.And(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) &
          getExpressionBuckets(right, bucketColumnName, numBuckets)
      case expressions.Or(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) |
        getExpressionBuckets(right, bucketColumnName, numBuckets)
      case _ =>
        val matchedBuckets = new BitSet(numBuckets)
        matchedBuckets.setUntil(numBuckets)
        matchedBuckets
    }
  }

  private def genBucketSet(
      normalizedFilters: Seq[Expression],
      bucketSpec: BucketSpec): Option[BitSet] = {
    if (normalizedFilters.isEmpty) {
      return None
    }

    val bucketColumnName = bucketSpec.bucketColumnNames.head
    val numBuckets = bucketSpec.numBuckets

    val normalizedFiltersAndExpr = normalizedFilters
      .reduce(expressions.And)
    val matchedBuckets = getExpressionBuckets(normalizedFiltersAndExpr, bucketColumnName,
      numBuckets)

    val numBucketsSelected = matchedBuckets.cardinality()

    logInfo(log"Pruned ${MDC(NUM_PRUNED, numBuckets - numBucketsSelected)} " +
      log"out of ${MDC(TOTAL, numBuckets)} buckets.")

    // None means all the buckets need to be scanned
    if (numBucketsSelected == numBuckets) {
      None
    } else {
      Some(matchedBuckets)
    }
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ScanOperation(projects, stayUpFilters, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table, _)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      val filtersToPush = filters.filter(f =>
          DataSourceUtils.shouldPushFilter(f, fsRelation.fileFormat.supportsCollationPushDown))

      val normalizedFilters = DataSourceStrategy.normalizeExprs(
        filtersToPush, l.output)

      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)

      // this partitionKeyFilters should be the same with the ones being executed in
      // PruneFileSourcePartitions
      val partitionKeyFilters = DataSourceStrategy.getPushedDownFilters(partitionColumns,
        normalizedFilters)

      val bucketSpec: Option[BucketSpec] = fsRelation.bucketSpec
      val bucketSet = if (shouldPruneBuckets(bucketSpec)) {
        genBucketSet(normalizedFilters, bucketSpec.get)
      } else {
        None
      }

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      // `dataColumns` might have partition columns, we need to filter them out.
      val dataColumnsWithoutPartitionCols = dataColumns.filterNot(partitionSet.contains)
      // Scalar subquery can be pushed down as data filter at runtime, since we always
      // execute subquery first.
      // It has no meaning to push down bloom filter, so skip it.
      val normalizedFiltersWithScalarSubqueries = normalizedFilters
        .filterNot(e => e.containsPattern(PLAN_EXPRESSION) && !e.containsPattern(SCALAR_SUBQUERY))
        .filterNot(_.isInstanceOf[BloomFilterMightContain])
      val dataFilters = normalizedFiltersWithScalarSubqueries.flatMap { f =>
        if (f.references.intersect(partitionSet).nonEmpty) {
          extractPredicatesWithinOutputSet(f, AttributeSet(dataColumnsWithoutPartitionCols))
        } else {
          Some(f)
        }
      }
      val supportNestedPredicatePushdown =
        DataSourceUtils.supportNestedPredicatePushdown(fsRelation)
      val pushedFilters = dataFilters
        .flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
      logInfo(log"Pushed Filters: ${MDC(PUSHED_FILTERS, pushedFilters.mkString(","))}")

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
      logInfo(log"Post-Scan Filters: ${MDC(POST_SCAN_FILTERS, afterScanFilters.mkString(","))}")

      val filterAttributes = AttributeSet(afterScanFilters ++ stayUpFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns = dataColumnsWithoutPartitionCols
        .filter(requiredAttributes.contains)

      // Metadata attributes are part of a column of type struct up to this point. Here we extract
      // this column from the schema and specify a matcher for that.
      object MetadataStructColumn {
        // The column returned by [[FileFormat.createFileMetadataCol]] is sanitized and lacks
        // the internal metadata we rely on here. Map back to the real fields by field name.
        lazy val availableMetadataFields = fsRelation.fileFormat.metadataSchemaFields
          .map(field => field.name.toLowerCase(Locale.ROOT) -> field).toMap

        def unapply(attributeReference: AttributeReference): Option[AttributeReference] = {
          attributeReference match {
            case attr @ FileSourceMetadataAttribute(
                MetadataAttributeWithLogicalName(
                  AttributeReference(_, schema: StructType, _, _),
                  FileFormat.METADATA_NAME)) =>
              val adjustedFields = schema.fields.map { field =>
                val metadata = availableMetadataFields(field.name.toLowerCase(Locale.ROOT)).metadata
                field.copy(metadata = metadata)
              }
              Some(attr.withDataType(StructType(adjustedFields)))
            case _ => None
          }
        }
      }

      val metadataStructOpt = l.output.collectFirst {
        case MetadataStructColumn(attr) => attr
      }

      // Track constant and generated columns separately, because they are used differently in code
      // below. Also remember the attribute for each logical column name, so we can map back to it.
      val constantMetadataColumns = mutable.Buffer.empty[Attribute]
      val generatedMetadataColumns = mutable.Buffer.empty[Attribute]
      val metadataColumnsByName = mutable.Map.empty[String, Attribute]

      metadataStructOpt.foreach { metadataStruct =>
        val schemaColumns = (readDataColumns ++ partitionColumns)
          .map(_.name.toLowerCase(Locale.ROOT))
          .toSet

        metadataStruct.dataType.asInstanceOf[StructType].fields.foreach {
          case FileSourceGeneratedMetadataStructField(field, internalName) =>
            if (schemaColumns.contains(internalName)) {
              throw new AnalysisException(
                errorClass = "_LEGACY_ERROR_TEMP_3069",
                messageParameters = Map(
                  "internalName" -> internalName,
                  "colName" -> s"${FileFormat.METADATA_NAME}.${field.name}"
                ))
            }

            // NOTE: Readers require the internal column to be nullable because it's not part of the
            // file's public schema. The projection below will restore the correct nullability for
            // the column while constructing the final metadata struct.
            val attr = DataTypeUtils.toAttribute(field.copy(internalName, nullable = true))
            metadataColumnsByName.put(field.name, attr)
            generatedMetadataColumns += attr

          case FileSourceConstantMetadataStructField(field) =>
            val attr = DataTypeUtils.toAttribute(field)
            metadataColumnsByName.put(field.name, attr)
            constantMetadataColumns += attr

          case field => throw new AnalysisException(
            errorClass = "_LEGACY_ERROR_TEMP_3070",
            messageParameters = Map("field" -> field.toString))
        }
      }

      val outputDataSchema = (readDataColumns ++ generatedMetadataColumns).toStructType

      // The output rows will be produced during file scan operation in three steps:
      //  (1) File format reader populates a `Row` with `readDataColumns` and
      //      `fileFormatReaderGeneratedMetadataColumns`
      //  (2) Then, a row containing `partitionColumns` is joined at the end.
      //  (3) Finally, a row containing `fileConstantMetadataColumns` is also joined at the end.
      // By placing `fileFormatReaderGeneratedMetadataColumns` before `partitionColumns` and
      // `fileConstantMetadataColumns` in the `outputAttributes` we make these row operations
      // simpler and more efficient.
      val outputAttributes = readDataColumns ++ generatedMetadataColumns ++
        partitionColumns ++ constantMetadataColumns

      // Rebind metadata attribute references in filters after the metadata attribute struct has
      // been flattened. Only data filters can contain metadata references. After the rebinding
      // all references will be bound to output attributes which are either
      // [[FileSourceConstantMetadataAttribute]] or [[FileSourceGeneratedMetadataAttribute]] after
      // the flattening from the metadata struct.
      def rebindFileSourceMetadataAttributesInFilters(filters: Seq[Expression]): Seq[Expression] =
        filters.map { filter =>
          filter.transform {
            // Replace references to the _metadata column. This will affect references to the column
            // itself but also where fields from the metadata struct are used.
            case MetadataStructColumn(AttributeReference(_, fields: StructType, _, _)) =>
              val reboundFields = fields.map(field => metadataColumnsByName(field.name))
              CreateStruct(reboundFields)
          }.transform {
            // Replace references to struct fields with the field values. This is to avoid creating
            // temporaries to improve performance.
            case GetStructField(createNamedStruct: CreateNamedStruct, ordinal, _) =>
              createNamedStruct.valExprs(ordinal)
          }
        }

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputDataSchema,
          partitionKeyFilters.toSeq,
          bucketSet,
          None,
          rebindFileSourceMetadataAttributesInFilters(dataFilters),
          table.map(_.identifier))

      // extra Project node: wrap flat metadata columns to a metadata struct
      val withMetadataProjections = metadataStructOpt.map { metadataStruct =>
        val structColumns = metadataStruct.dataType.asInstanceOf[StructType].fields.map { field =>
          // Construct the metadata struct the query expects to see, using the columns we previously
          // created. Be sure to restore the proper name and nullability for each metadata field.
          metadataColumnsByName(field.name).withName(field.name).withNullability(field.nullable)
        }
        // SPARK-41151: metadata column is not nullable for file sources.
        // Here, we *explicitly* enforce the not null to `CreateStruct(structColumns)`
        // to avoid any risk of inconsistent schema nullability
        val metadataAlias =
          Alias(KnownNotNull(CreateStruct(structColumns.toImmutableArraySeq)),
            FileFormat.METADATA_NAME)(exprId = metadataStruct.exprId)
        execution.ProjectExec(
          readDataColumns ++ partitionColumns :+ metadataAlias, scan)
      }.getOrElse(scan)

      // bottom-most filters are put in the left of the list.
      val finalFilters = afterScanFilters.toSeq.reduceOption(expressions.And).toSeq ++ stayUpFilters
      val withFilter = finalFilters.foldLeft(withMetadataProjections)((plan, cond) => {
        execution.FilterExec(cond, plan)
      })
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
}
