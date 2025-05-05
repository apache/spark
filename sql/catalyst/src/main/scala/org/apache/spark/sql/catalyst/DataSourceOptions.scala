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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Interface defines the following methods for a data source:
 *  - register a new option name
 *  - retrieve all registered option names
 *  - valid a given option name
 *  - get alternative option name if any
 */
trait DataSourceOptions {
  // Option -> Alternative Option if any
  private val validOptions = collection.mutable.Map[String, Option[String]]()

  /**
   * Register a new Option.
   */
  protected def newOption(name: String): String = {
    validOptions += (name -> None)
    name
  }

  /**
   * Register a new Option with an alternative name.
   * @param name Option name
   * @param alternative Alternative option name
   */
  protected def newOption(name: String, alternative: String): Unit = {
    // Register both of the options
    validOptions += (name -> Some(alternative))
    validOptions += (alternative -> Some(name))
  }

  /**
   * @return All data source options and their alternatives if any
   */
  def getAllOptions: scala.collection.Set[String] = validOptions.keySet

  /**
   * @param name Option name to be validated
   * @return if the given Option name is valid
   */
  def isValidOption(name: String): Boolean = validOptions.contains(name)

  /**
   * @param name Option name
   * @return Alternative option name if any
   */
  def getAlternativeOption(name: String): Option[String] = validOptions.get(name).flatten
}

object DataSourceOptions {
  // The common option name for all data sources that supports single-variant-column parsing mode.
  // The option should take in a column name and specifies that the entire record should be stored
  // as a single VARIANT type column in the table with the given column name.
  // E.g. spark.read.format("<data-source-format>").option("singleVariantColumn", "colName")
  val SINGLE_VARIANT_COLUMN = "singleVariantColumn"
  // The common option name for all data sources that supports corrupt record. In case of a parsing
  // error, the record will be stored as a string in the column with the given name.
  // Theoretically, the behavior of this option is not affected by the parsing mode
  // (PERMISSIVE/FAILFAST/DROPMALFORMED). However, the corrupt record is only visible to the user
  // when in PERMISSIVE mode, because the queries will fail in FAILFAST mode, or the row containing
  // the corrupt record will be dropped in DROPMALFORMED mode.
  val COLUMN_NAME_OF_CORRUPT_RECORD = "columnNameOfCorruptRecord"

  // When `singleVariantColumn` is enabled and there is a user-specified schema, the schema must
  // either be a variant field, or a variant field plus a corrupt column field.
  def validateSingleVariantColumn(
      options: CaseInsensitiveMap[String],
      userSpecifiedSchema: Option[StructType]): Unit = {
    (options.get(SINGLE_VARIANT_COLUMN), userSpecifiedSchema) match {
      case (Some(variantColumnName), Some(schema)) =>
        var valid = schema.fields.exists { f =>
          f.dataType.isInstanceOf[VariantType] && f.name == variantColumnName && f.nullable
        }
        schema.length match {
          case 1 =>
          case 2 =>
            val corruptRecordColumnName = options.getOrElse(
              COLUMN_NAME_OF_CORRUPT_RECORD, SQLConf.get.columnNameOfCorruptRecord)
            valid = valid && corruptRecordColumnName != variantColumnName
            valid = valid && schema.fields.exists { f =>
              f.dataType.isInstanceOf[StringType] && f.name == corruptRecordColumnName && f.nullable
            }
          case _ => valid = false
        }
        if (!valid) {
          throw QueryCompilationErrors.invalidSingleVariantColumn(schema)
        }
      case _ =>
    }
  }
}
