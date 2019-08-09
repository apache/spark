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

package org.apache.spark.sql.catalog.v2

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.v2.expressions.{BucketTransform, IdentityTransform, LogicalExpressions, Transform}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.types.StructType

/**
 * Conversion helpers for working with v2 [[CatalogPlugin]].
 */
object CatalogV2Implicits {
  implicit class PartitionTypeHelper(partitionType: StructType) {
    def asTransforms: Array[Transform] = partitionType.names.map(LogicalExpressions.identity)
  }

  implicit class BucketSpecHelper(spec: BucketSpec) {
    def asTransform: BucketTransform = {
      if (spec.sortColumnNames.nonEmpty) {
        throw new AnalysisException(
          s"Cannot convert bucketing with sort columns to a transform: $spec")
      }

      LogicalExpressions.bucket(spec.numBuckets, spec.bucketColumnNames: _*)
    }
  }

  implicit class TransformHelper(transforms: Seq[Transform]) {
    def asPartitionColumns: Seq[String] = {
      val (idTransforms, nonIdTransforms) = transforms.partition(_.isInstanceOf[IdentityTransform])

      if (nonIdTransforms.nonEmpty) {
        throw new AnalysisException("Transforms cannot be converted to partition columns: " +
            nonIdTransforms.map(_.describe).mkString(", "))
      }

      idTransforms.map(_.asInstanceOf[IdentityTransform]).map(_.reference).map { ref =>
        val parts = ref.fieldNames
        if (parts.size > 1) {
          throw new AnalysisException(s"Cannot partition by nested column: $ref")
        } else {
          parts(0)
        }
      }
    }
  }

  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asTableCatalog: TableCatalog = plugin match {
      case tableCatalog: TableCatalog =>
        tableCatalog
      case _ =>
        throw new AnalysisException(s"Cannot use catalog ${plugin.name}: not a TableCatalog")
    }
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quote).mkString(".")
  }

  implicit class IdentifierHelper(ident: Identifier) {
    def quoted: String = {
      if (ident.namespace.nonEmpty) {
        ident.namespace.map(quote).mkString(".") + "." + quote(ident.name)
      } else {
        quote(ident.name)
      }
    }
  }

  implicit class MultipartIdentifierHelper(namespace: Seq[String]) {
    def quoted: String = namespace.map(quote).mkString(".")
  }

  private def quote(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }
}
