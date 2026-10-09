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

package org.apache.spark.sql.catalyst.json

import scala.util.control.NonFatal

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * CHAR/VARCHAR assignment for JSON, outside [[JacksonParser]].
 *
 * Same split as Hive TRANSFORM (SPARK-60090): the parser sees unbounded STRING, then
 * write-side assignment runs on the parsed value. [[JacksonParser]] stays CHAR-unaware.
 */
private[sql] object JsonParseAssignment {

  def parserSchema(schema: DataType): DataType = {
    if (needsAssignment(schema)) {
      CharVarcharUtils.replaceCharVarcharWithStringAlways(schema)
    } else {
      schema
    }
  }

  def pushedFilters(declared: StructType, filters: Seq[Filter]): Seq[Filter] = {
    if (!needsAssignment(declared)) {
      filters
    } else {
      filters.filterNot { f =>
        f.references.exists { name =>
          declared.getFieldIndex(name).exists { i =>
            CharVarcharUtils.hasCharVarchar(declared.fields(i).dataType)
          }
        }
      }
    }
  }

  def parse(
      rows: => Iterable[InternalRow],
      declared: DataType,
      recordLiteral: () => UTF8String): Iterable[InternalRow] = {
    rowAssigner(declared) match {
      case None => rows
      case Some(assign) =>
        try {
          rows.map(row => assignRow(assign, row, recordLiteral, recoverable = false))
        } catch {
          case e: SparkUpgradeException => throw e
          case DuplicateMapKeyUtils(e) => throw e
          case e: BadRecordException =>
            throw e.copy(partialResults = () => assignPartials(assign, e.partialResults()))
        }
    }
  }

  def parseIterator(
      rows: Iterator[InternalRow],
      declared: DataType,
      recordLiteral: () => UTF8String): Iterator[InternalRow] = {
    rowAssigner(declared) match {
      case None => rows
      case Some(assign) =>
        new Iterator[InternalRow] {
          override def hasNext: Boolean = withAssignedPartials(assign)(rows.hasNext)
          override def next(): InternalRow = withAssignedPartials(assign) {
            assignRow(assign, rows.next(), recordLiteral, recoverable = true)
          }
        }
    }
  }

  private def needsAssignment(schema: DataType): Boolean = {
    SQLConf.get.charVarcharStandardSemantics && CharVarcharUtils.hasCharVarchar(schema)
  }

  private def rowAssigner(declared: DataType): Option[InternalRow => InternalRow] = {
    if (!needsAssignment(declared)) {
      None
    } else {
      val physical = CharVarcharUtils.replaceCharVarcharWithStringAlways(declared)
      val expr = CharVarcharUtils.assignAfterParse(
        BoundReference(0, physical, nullable = true),
        declared)
      Some(declared match {
        case _: StructType =>
          (row: InternalRow) => expr.eval(InternalRow(row)).asInstanceOf[InternalRow]
        case _ =>
          (row: InternalRow) =>
            InternalRow(expr.eval(InternalRow(row.get(0, physical))))
      })
    }
  }

  private def withAssignedPartials[A](assign: InternalRow => InternalRow)(op: => A): A = {
    try {
      op
    } catch {
      case e: BadRecordException =>
        throw e.copy(partialResults = () => assignPartials(assign, e.partialResults()))
    }
  }

  private def assignRow(
      assign: InternalRow => InternalRow,
      row: InternalRow,
      recordLiteral: () => UTF8String,
      recoverable: Boolean): InternalRow = {
    try {
      assign(row)
    } catch {
      case e: SparkUpgradeException => throw e
      case DuplicateMapKeyUtils(e) => throw e
      case e: RuntimeException =>
        throw BadRecordException(recordLiteral, () => Array.empty, e, recoverable)
    }
  }

  private def assignPartials(
      assign: InternalRow => InternalRow,
      partials: Array[InternalRow]): Array[InternalRow] = {
    try {
      partials.map(assign)
    } catch {
      case e: SparkUpgradeException => throw e
      case DuplicateMapKeyUtils(e) => throw e
      case NonFatal(_) => Array.empty
    }
  }
}
