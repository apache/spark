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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * An interface for mapping two different schemas. For the relations that have are backed by files,
 * the inferred schema from the files might be different with the schema stored in the catalog. In
 * such case, the interface helps mapping inconsistent schemas.
 *
 * It assumes that the length of the fields in the schema inferred from the files plus the fields
 * of partition are the same as the length of the catalog schema. Besides, the field order of
 * (data schema + partition schema) must be the same as the catalog schema.
 *
 * For example, if Hive stores ORC files with incorrect schema ['_col1': int, '_col2': string], but
 * the schema stored in Hive metastore is correct: ['a': int, 'b': string]. We need to use
 * the catalog schema ['a', 'b'] as relation output. But we must use ['_col1', '_col2'] when reading
 * data from the files. Besides, if we want to pushdown filters to data reading, we need to
 * transform the fields in the filter expressions which are referring to catalog schema to the
 * fields in data schema. E.g., a filter expression for the condition 'a=1' will be
 * Equal(AttributeReference('a'), Literal(1)), and we will transform it to
 * Equal(AttributeReference('_col1', Literal(1))), otherwise the filter pushdown will be failed.
 */
private[sql] trait SchemaMapping {
  /** The schema inferred from the files. */
  val dataSchema: StructType

  /** The schema used in partition. */
  val partitionSchema: StructType

  /** The schema fetched from the catalog. */
  val catalogSchema: StructType

  require(catalogSchema.length == 0 ||
    dataSchema.merge(partitionSchema).length == catalogSchema.length,
    s"The data schema in files: $dataSchema plus the partition schema: $partitionSchema " +
      s"should have the same number of fields with the schema in catalog: $catalogSchema.")

  /** Returns the correspond catalog field for the given data field. */
  def lookForFieldFromDataField(field: StructField): Option[StructField] = {
    if (catalogSchema.fields.length == 0) {
      None
    } else {
      dataSchema.getFieldIndex(field.name).map { idx =>
        catalogSchema.fields(idx)
      }
    }
  }

  /** Returns the correspond data field for the given catalog field. */
  def lookForFieldFromCatalogField(field: StructField): Option[StructField] = {
    catalogSchema.getFieldIndex(field.name).map { idx =>
      dataSchema.fields(idx)
    }
  }

  /** Returns the correspond data field for the given catalog field. */
  def lookForFieldFromCatalogField(fieldName: String): Option[StructField] = {
    catalogSchema.getFieldIndex(fieldName).map { idx =>
      dataSchema.fields(idx)
    }
  }

  /**
   * Transforms the attributes in the given expression which is based on the catalog schema
   * to corresponding attributes in the schema in the files.
   */
  def transformExpressionToUseDataSchema(expr: Expression): Expression = {
    expr transform {
      case a: AttributeReference =>
        lookForFieldFromCatalogField(a.name).map { field =>
          a.withName(field.name)
        }.getOrElse(a)
    }
  }
}
