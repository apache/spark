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

package org.apache.spark.sql.csv

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.sql.Logging

private[sql] object CsvRDD extends Logging {

  /**
   * Infers schema of a CSV file. It uses the first row of the first partition to
   * infer number of columns. If header flag is set, all lines that equal the first line
   * are filtered before parsing.
   *
   * o If a line contains fewer tokens than the schema, it is padded with nulls
   * o If a line has more tokens than the schema, extra tokens are ignored.
   */
  private[sql] def inferSchema(
      csv: RDD[String],
      delimiter: String,
      quote: Char,
      userSchema: Option[StructType],
      useHeader: Boolean): LogicalPlan = {

    val firstLine = csv.first()
    val schema = userSchema match {
      case Some(userSupportedSchema) => userSupportedSchema
      case None =>
        // Assume first row is representative and use it to determine number of fields
        val firstRow = new CsvTokenizer(Seq(firstLine).iterator, delimiter, quote).next()
        val header = if (useHeader) {
          logger.info(s"Using header line: $firstLine")
          firstRow
        } else {
          firstRow.zipWithIndex.map { case (value, index) => s"V$index"}
        }
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName, StringType, nullable = true)
        }
        StructType(schemaFields)
    }

    val numFields = schema.fields.length
    logger.info(s"Parsing CSV with $numFields.")
    val row = new GenericMutableRow(numFields)
    val projection = schemaCaster(asAttributes(schema))

    val parsedCSV = csv.mapPartitions { iter =>
      // When using header, any input line that equals firstLine is assumed to be header
      val csvIter = if (useHeader) {
        iter.filter(_ != firstLine)
      } else {
        iter
      }
      val tokenIter = new CsvTokenizer(csvIter, delimiter, quote)
      parseCSV(tokenIter, schema.fields, projection, row)
    }

    SparkLogicalPlan(ExistingRdd(asAttributes(schema), parsedCSV))
  }

  protected def schemaCaster(schema: Seq[AttributeReference]): MutableProjection = {
    val startSchema = (1 to schema.length).toSeq.map(
      index => new AttributeReference(s"V$index", StringType, nullable = true)())
    val casts = schema.zipWithIndex.map { case (ar, i) => Cast(startSchema(i), ar.dataType) }
    new MutableProjection(casts, startSchema)
  }

  private def parseCSV(
      iter: Iterator[Array[String]],
      schemaFields: Seq[StructField],
      projection: MutableProjection,
      row: GenericMutableRow): Iterator[Row] = {
    iter.map { tokens =>
      schemaFields.zipWithIndex.foreach {
        case (StructField(name, dataType, _), index) =>
          row.update(index, tokens(index))
      }
      projection(row)
    }
  }

  private def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(field => AttributeReference(field.name, field.dataType, nullable = true)())
  }
}

