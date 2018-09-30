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

package org.apache.spark.sql.catalyst.expressions

import java.io.CharArrayWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions, UnivocityGenerator}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts a [[StructType]] to a CSV output string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr[, options]) - Returns a CSV string with a given struct value",
  examples = """
    Examples:
      > SELECT _FUNC_(named_struct('a', 1, 'b', 2));
       1,2
      > SELECT _FUNC_(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
       "26/08/2015"
  """,
  since = "2.5.0")
// scalastyle:on line.size.limit
case class StructsToCsv(
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with CodegenFallback with ExpectsInputTypes {
  override def nullable: Boolean = true

  def this(options: Map[String, String], child: Expression) = this(options, child, None)

  // Used in `FunctionRegistry`
  def this(child: Expression) = this(Map.empty, child, None)
  def this(child: Expression, options: Expression) =
    this(
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  @transient
  lazy val writer = new CharArrayWriter()

  @transient
  lazy val inputSchema: StructType = child.dataType match {
    case st: StructType => st
    case other =>
      throw new IllegalArgumentException(s"Unsupported input type ${other.catalogString}")
  }

  @transient
  lazy val gen = new UnivocityGenerator(
    inputSchema, writer, new CSVOptions(options, columnPruning = true, timeZoneId.get))

  // This converts rows to the CSV output according to the given schema.
  @transient
  lazy val converter: Any => UTF8String = {
    def getAndReset(): UTF8String = {
      gen.flush()
      val csv = writer.toString
      writer.reset()
      UTF8String.fromString(csv)
    }

    (row: Any) =>
      gen.write(row.asInstanceOf[InternalRow])
      getAndReset()
  }

  override def dataType: DataType = StringType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(value: Any): Any = converter(value)

  override def inputTypes: Seq[AbstractDataType] = TypeCollection(StructType) :: Nil
}
