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
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, AttributeReference, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.sql.Logging

private[sql] object CsvRDD extends Logging {

  private[sql] def inferSchema(
      csv: RDD[String],
      delimiter: String = ",",
      useHeader: Boolean = false): LogicalPlan = {

    // TODO: Read header. For now assume there is no header
    // TODO: What if first row is not representative
    val firstLine = csv.first()
    val firstRow = firstLine.split(delimiter)
    val header = if (useHeader) {
      firstRow
    } else {
      firstRow.zipWithIndex.map { case (value, index) => s"V$index" }
    }

    // TODO: Infer types based on a sample
    // TODO: Figure out a way for user to specify types/schema
    val fields = header.map( fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val parsedCSV = csv.mapPartitions { iter =>
      val csvIter = if (useHeader) {
        // Any input line that equals the headerLine is assumed to be header and filtered
        iter.filter(_ != firstLine)
      } else {
        iter
      }
      parseCSV(csvIter, delimiter, schema)
    }

    SparkLogicalPlan(ExistingRdd(asAttributes(schema), parsedCSV))
  }

  private def castToType(value: Any, dataType: DataType): Any = dataType match {
    case StringType => value.asInstanceOf[String]
    case BooleanType => value.asInstanceOf[Boolean]
    case DoubleType => value.asInstanceOf[Double]
    case FloatType => value.asInstanceOf[Float]
    case IntegerType => value.asInstanceOf[Int]
    case LongType => value.asInstanceOf[Long]
    case ShortType => value.asInstanceOf[Short]
    case _ => null
  }

  private def parseCSV(iter: Iterator[String],
      delimiter: String,
      schema: StructType): Iterator[Row] = {
    val row = new GenericMutableRow(schema.fields.length)
    iter.map { line =>
      val tokens = line.split(delimiter)
      schema.fields.zipWithIndex.foreach {
        case (StructField(name, dataType, _), index) =>
          row.update(index, castToType(tokens(index), dataType))
      }
      row
    }
  }

  private def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(field => AttributeReference(field.name, field.dataType, nullable = true)())
  }

}