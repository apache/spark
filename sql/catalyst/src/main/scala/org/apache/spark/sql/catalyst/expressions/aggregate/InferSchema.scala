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

package org.apache.spark.sql.catalyst.expressions.aggregate

import scala.util.Try

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionDescription, JsonExprUtils}
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JsonInferSchema, JSONOptions}
import org.apache.spark.sql.catalyst.json.JsonInferSchema.compatibleRootType
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = """_FUNC_(expr,[options]) - Infers schema for JSON `expr` by using JSON `options`.""")
case class InferSchema(
  child: Expression,
  inputFormat: String,
  options: Map[String, String],
  override val mutableAggBufferOffset: Int,
  override val inputAggBufferOffset: Int) extends ImperativeAggregate {

  require(inputFormat.toLowerCase == "json", "Only JSON format is supported")

  def this(child: Expression) = {
    this(
      child = child,
      inputFormat = "json",
      options = Map.empty[String, String],
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  def this(child: Expression, options: Expression) = {
    this(
      child = child,
      inputFormat = "json",
      options = JsonExprUtils.convertToMapData(options),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(child)
  override def dataType: DataType = StringType
  override lazy val deterministic: Boolean = false
  override def prettyName: String = "infer_schema"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val aggBufferAttributes: Seq[AttributeReference] = {
    Seq(AttributeReference("infer_schema_agg_buffer", StringType)())
  }
  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] = {
    aggBufferAttributes.map(_.newInstance())
  }
  override val aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)
  private val indexOfSchemaField = 0

  override def initialize(mutableAggBuffer: InternalRow): Unit = {
    mutableAggBuffer.setNullAt(mutableAggBufferOffset + indexOfSchemaField)
  }

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    val inputValue = child.eval(inputRow)
    if (inputValue != null) {
      val currentSchema = inferSchema(inputValue)
      val bufferOffset = mutableAggBufferOffset + indexOfSchemaField
      val mergedSchema = if (mutableAggBuffer.isNullAt(bufferOffset)) {
        currentSchema
      } else {
        val inferredSchema = mutableAggBuffer.getUTF8String(bufferOffset)
        mergeSchemas(inferredSchema, currentSchema)
      }

      mergedSchema.foreach(schema => mutableAggBuffer.update(bufferOffset, schema))
    }
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    val inputOffset = inputAggBufferOffset + indexOfSchemaField
    if (!inputAggBuffer.isNullAt(inputOffset)) {
      val inputSchema = Some(inputAggBuffer.getUTF8String(inputOffset))
      val bufferOffset = mutableAggBufferOffset + indexOfSchemaField
      val mergedSchema = if (mutableAggBuffer.isNullAt(bufferOffset)) {
        inputSchema
      } else {
        val bufferSchema = mutableAggBuffer.getUTF8String(bufferOffset)
        mergeSchemas(bufferSchema, inputSchema)
      }

      mergedSchema.foreach(schema => mutableAggBuffer.update(bufferOffset, schema))
    }
  }

  override def eval(input: InternalRow = null): Any = {
    if (input == null) {
      null
    } else {
      input.getUTF8String(indexOfSchemaField)
    }
  }

  private val jsonOptions = new JSONOptions(
    parameters = options + ("mode" -> DropMalformedMode.name),
    defaultTimeZoneId = "UTC")
  private val jsonFactory = new JsonFactory()

  private def inferSchema(input: Any): Option[UTF8String] = input match {
    case jsonRow: UTF8String =>
      val dataType = JsonInferSchema.inferForRow(
        row = jsonRow,
        configOptions = jsonOptions,
        createParser = CreateJacksonParser.utf8String,
        factory = jsonFactory)

      dataType.map(dt => UTF8String.fromString(dt.catalogString))
    case other =>
      throw new IllegalArgumentException(s"Wrong input type ${other.getClass.getCanonicalName}")
  }

  private val typeMerger = compatibleRootType(
    jsonOptions.columnNameOfCorruptRecord,
    jsonOptions.parseMode)

  private def mergeSchemas(
      inferredSchema: UTF8String,
      currentSchema: Option[UTF8String]): Option[UTF8String] = {
    currentSchema.flatMap { schema =>
      Try {
        val inferredType = DataType.fromDDL(inferredSchema.toString)
        val currentType = DataType.fromDDL(schema.toString)

        typeMerger(inferredType, currentType)
      }.map(dt => UTF8String.fromString(dt.catalogString)).toOption
    }
  }
}
