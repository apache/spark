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
      delimiter: String,
      quote: String,
      useHeader: Boolean): LogicalPlan = {

    // Constructing schema
    // TODO: Infer types based on a sample and/or let user specify types/schema
    val firstLine = csv.first()
    // Assuming first row is representative and using it to determine number of fields
    val firstRow = new CsvTokenizer(Seq(firstLine).iterator, delimiter, quote).next()
    val header = if (useHeader) {
      firstRow
    } else {
      firstRow.zipWithIndex.map { case (value, index) => s"V$index" }
    }

    val schemaFields = header.map { fieldName =>
      StructField(fieldName.asInstanceOf[String], StringType, nullable = true)
    }
    val schema = StructType(schemaFields)

    val parsedCSV = csv.mapPartitions { iter =>
      // When using header, any input line that equals firstLine is assumed to be header
      val csvIter = if (useHeader) {
        iter.filter(_ != firstLine)
      } else {
        iter
      }
      val tokenIter = new CsvTokenizer(csvIter, delimiter, quote)
      parseCSV(tokenIter, schema)
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

  private def parseCSV(iter: Iterator[Array[Any]], schema: StructType): Iterator[Row] = {
    val row = new GenericMutableRow(schema.fields.length)
    iter.map { tokens =>
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

