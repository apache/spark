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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils


abstract class ParquetFileSplitter {
  def buildSplitter(filters: Seq[Filter]): (FileStatus => Seq[FileSplit])

  def singleFileSplit(stat: FileStatus): Seq[FileSplit] = {
    Seq(new FileSplit(stat.getPath, 0, stat.getLen, Array.empty))
  }
}

object ParquetDefaultFileSplitter extends ParquetFileSplitter {
  override def buildSplitter(filters: Seq[Filter]): (FileStatus => Seq[FileSplit]) = {
    stat => singleFileSplit(stat)
  }
}

class ParquetMetadataFileSplitter(
    val root: Path,
    val blocks: Seq[BlockMetaData],
    val schema: StructType,
    val session: SparkSession)
  extends ParquetFileSplitter
  with Logging {

  private val int96AsTimestamp = session.sessionState.conf.isParquetINT96AsTimestamp

  private val referencedFiles = blocks.map(bmd => new Path(root, bmd.getPath)).toSet

  private val filterSets: Cache[Filter, RoaringBitmap] =
    CacheBuilder.newBuilder()
      .expireAfterAccess(4, TimeUnit.HOURS)
      .concurrencyLevel(1)
      .build()

  override def buildSplitter(filters: Seq[Filter]): (FileStatus => Seq[FileSplit]) = {
    val (applied, unapplied, filteredBlocks) = this.synchronized {
      val (applied, unapplied) = filters.partition(filterSets.getIfPresent(_) != null)
      val filteredBlocks = filterSets.getAllPresent(applied.asJava).values().asScala
        .reduceOption(RoaringBitmap.and)
        .map { bitmap =>
          blocks.zipWithIndex.filter { case(block, index) =>
            bitmap.contains(index)
          }.map(_._1)
        }.getOrElse(blocks)
      (applied, unapplied, filteredBlocks)
    }

    val eligible = applyParquetFilter(unapplied, filteredBlocks).map { bmd =>
      val blockPath = new Path(root, bmd.getPath)
      new FileSplit(blockPath, bmd.getStartingPos, bmd.getCompressedSize, Array.empty)
    }

    val statFilter: (FileStatus => Seq[FileSplit]) = { stat =>
      if (referencedFiles.contains(stat.getPath)) {
        eligible.filter(_.getPath == stat.getPath)
      } else {
        log.warn(s"Found _metadata file for $root," +
          s" but no entries for blocks in ${stat.getPath}. Retaining whole file.")
        singleFileSplit(stat)
      }
    }
    statFilter
  }

  private def applyParquetFilter(
      filters: Seq[Filter],
      blocks: Seq[BlockMetaData]): Seq[BlockMetaData] = {
    val predicates = filters.flatMap {
      ParquetFilters.createFilter(schema, _, int96AsTimestamp)
    }
    if (predicates.nonEmpty) {
      // Asynchronously build bitmaps
      Future {
        buildFilterBitMaps(filters)
      }(ParquetMetadataFileSplitter.executionContext)

      val predicate = predicates.reduce(FilterApi.and)
      blocks.filterNot(bmd => StatisticsFilter.canDrop(predicate, bmd.getColumns))
    } else {
      blocks
    }
  }

  private def buildFilterBitMaps(filters: Seq[Filter]): Unit = {
    this.synchronized {
      // Only build bitmaps for filters that don't exist.
      val sets = filters
        .filter(filterSets.getIfPresent(_) == null)
        .flatMap { filter =>
          val bitmap = new RoaringBitmap
          ParquetFilters.createFilter(schema, filter, int96AsTimestamp)
            .map((filter, _, bitmap))
        }
      var i = 0
      val blockLen = blocks.size
      while (i < blockLen) {
        val bmd = blocks(i)
        sets.foreach { case (filter, parquetFilter, bitmap) =>
          if (!StatisticsFilter.canDrop(parquetFilter, bmd.getColumns)) {
            bitmap.add(i)
          }
        }
        i += 1
      }
      val mapping = sets.map { case (filter, _, bitmap) =>
        bitmap.runOptimize()
        filter -> bitmap
      }.toMap.asJava
      filterSets.putAll(mapping)
    }
  }
}
object ParquetMetadataFileSplitter {
  private val executionContext = ExecutionContext.fromExecutorService(
        ThreadUtils.newDaemonCachedThreadPool("parquet-metadata-filter", 1))
}
