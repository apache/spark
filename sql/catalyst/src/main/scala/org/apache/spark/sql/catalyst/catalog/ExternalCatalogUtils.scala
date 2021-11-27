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

package org.apache.spark.sql.catalyst.catalog

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Shell

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BasePredicate, BoundReference, Expression, Predicate}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ExternalCatalogUtils {
  // This duplicates default value of Hive `ConfVars.DEFAULTPARTITIONNAME`, since catalyst doesn't
  // depend on Hive.
  val DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__"

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // The following string escaping code is mainly copied from Hive (o.a.h.h.common.FileUtils).
  //////////////////////////////////////////////////////////////////////////////////////////////////

  val charToEscape = {
    val bitSet = new java.util.BitSet(128)

    /**
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    val clist = Array(
      '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
      '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
      '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
      '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
      '{', '[', ']', '^')

    clist.foreach(bitSet.set(_))

    if (Shell.WINDOWS) {
      Array(' ', '<', '>', '|').foreach(bitSet.set(_))
    }

    bitSet
  }

  def needsEscaping(c: Char): Boolean = {
    c >= 0 && c < charToEscape.size() && charToEscape.get(c)
  }

  def escapePathName(path: String): String = {
    val builder = new StringBuilder()
    path.foreach { c =>
      if (needsEscaping(c)) {
        builder.append('%')
        builder.append(f"${c.asInstanceOf[Int]}%02X")
      } else {
        builder.append(c)
      }
    }

    builder.toString()
  }


  def unescapePathName(path: String): String = {
    val sb = new StringBuilder
    var i = 0

    while (i < path.length) {
      val c = path.charAt(i)
      if (c == '%' && i + 2 < path.length) {
        val code: Int = try {
          Integer.parseInt(path.substring(i + 1, i + 3), 16)
        } catch {
          case _: Exception => -1
        }
        if (code >= 0) {
          sb.append(code.asInstanceOf[Char])
          i += 3
        } else {
          sb.append(c)
          i += 1
        }
      } else {
        sb.append(c)
        i += 1
      }
    }

    sb.toString()
  }

  def generatePartitionPath(
      spec: TablePartitionSpec,
      partitionColumnNames: Seq[String],
      tablePath: Path): Path = {
    val partitionPathStrings = partitionColumnNames.map { col =>
      getPartitionPathString(col, spec(col))
    }
    partitionPathStrings.foldLeft(tablePath) { (totalPath, nextPartPath) =>
      new Path(totalPath, nextPartPath)
    }
  }

  def getPartitionValueString(value: String): String = {
    if (value == null || value.isEmpty) {
      DEFAULT_PARTITION_NAME
    } else {
      escapePathName(value)
    }
  }

  def getPartitionPathString(col: String, value: String): String = {
    val partitionString = getPartitionValueString(value)
    escapePathName(col) + "=" + partitionString
  }

  def listPartitionsByFilter(
      conf: SQLConf,
      catalog: SessionCatalog,
      table: CatalogTable,
      partitionFilters: Seq[Expression]): Seq[CatalogTablePartition] = {
    if (conf.metastorePartitionPruning) {
      catalog.listPartitionsByFilter(table.identifier, partitionFilters)
    } else {
      ExternalCatalogUtils.prunePartitionsByFilter(table, catalog.listPartitions(table.identifier),
        partitionFilters, conf.sessionLocalTimeZone)
    }
  }

  def prunePartitionsByFilter(
      catalogTable: CatalogTable,
      inputPartitions: Seq[CatalogTablePartition],
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    if (predicates.isEmpty) {
      inputPartitions
    } else {
      val partitionSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
        catalogTable.partitionSchema)
      val boundPredicate = generatePartitionPredicateByFilter(catalogTable,
        partitionSchema, predicates)

      inputPartitions.filter { p =>
        boundPredicate.eval(p.toRow(partitionSchema, defaultTimeZoneId))
      }
    }
  }

  def generatePartitionPredicateByFilter(
      catalogTable: CatalogTable,
      partitionSchema: StructType,
      predicates: Seq[Expression]): BasePredicate = {
    val partitionColumnNames = catalogTable.partitionColumnNames.toSet

    val nonPartitionPruningPredicates = predicates.filterNot {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (nonPartitionPruningPredicates.nonEmpty) {
      throw QueryCompilationErrors.nonPartitionPruningPredicatesNotExpectedError(
        nonPartitionPruningPredicates)
    }

    Predicate.createInterpreted(predicates.reduce(And).transform {
      case att: AttributeReference =>
        val index = partitionSchema.indexWhere(_.name == att.name)
        BoundReference(index, partitionSchema(index).dataType, nullable = true)
    })
  }

  private def isNullPartitionValue(value: String): Boolean = {
    value == null || value == DEFAULT_PARTITION_NAME
  }

  /**
   * Returns true if `spec1` is a partial partition spec w.r.t. `spec2`, e.g. PARTITION (a=1) is a
   * partial partition spec w.r.t. PARTITION (a=1,b=2).
   */
  def isPartialPartitionSpec(
      spec1: TablePartitionSpec,
      spec2: TablePartitionSpec): Boolean = {
    spec1.forall {
      case (partitionColumn, value) if isNullPartitionValue(value) =>
        isNullPartitionValue(spec2(partitionColumn))
      case (partitionColumn, value) => spec2(partitionColumn) == value
    }
  }

  def convertNullPartitionValues(spec: TablePartitionSpec): TablePartitionSpec = {
    spec.mapValues(v => if (v == null) DEFAULT_PARTITION_NAME else v).map(identity).toMap
  }
}

object CatalogUtils {
  def normalizePartCols(
      tableName: String,
      tableCols: Seq[String],
      partCols: Seq[String],
      resolver: Resolver): Seq[String] = {
    partCols.map(normalizeColumnName(tableName, tableCols, _, "partition", resolver))
  }

  def normalizeBucketSpec(
      tableName: String,
      tableCols: Seq[String],
      bucketSpec: BucketSpec,
      resolver: Resolver): BucketSpec = {
    val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec
    val normalizedBucketCols = bucketColumnNames.map { colName =>
      normalizeColumnName(tableName, tableCols, colName, "bucket", resolver)
    }
    val normalizedSortCols = sortColumnNames.map { colName =>
      normalizeColumnName(tableName, tableCols, colName, "sort", resolver)
    }
    BucketSpec(numBuckets, normalizedBucketCols, normalizedSortCols)
  }

  /**
   * Convert URI to String.
   * Since URI.toString does not decode the uri, e.g. change '%25' to '%'.
   * Here we create a hadoop Path with the given URI, and rely on Path.toString
   * to decode the uri
   * @param uri the URI of the path
   * @return the String of the path
   */
  def URIToString(uri: URI): String = {
    new Path(uri).toString
  }

  /**
   * Convert String to URI.
   * Since new URI(string) does not encode string, e.g. change '%' to '%25'.
   * Here we create a hadoop Path with the given String, and rely on Path.toUri
   * to encode the string
   * @param str the String of the path
   * @return the URI of the path
   */
  def stringToURI(str: String): URI = {
    new Path(str).toUri
  }

  def makeQualifiedNamespacePath(
      locationUri: URI,
      warehousePath: String,
      hadoopConf: Configuration): URI = {
    if (locationUri.isAbsolute) {
      locationUri
    } else {
      val fullPath = new Path(warehousePath, CatalogUtils.URIToString(locationUri))
      makeQualifiedPath(fullPath.toUri, hadoopConf)
    }
  }

  def makeQualifiedPath(path: URI, hadoopConf: Configuration): URI = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri
  }

  private def normalizeColumnName(
      tableName: String,
      tableCols: Seq[String],
      colName: String,
      colType: String,
      resolver: Resolver): String = {
    tableCols.find(resolver(_, colName)).getOrElse {
      throw QueryCompilationErrors.columnNotDefinedInTableError(
        colType, colName, tableName, tableCols)
    }
  }
}
