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

package org.apache.spark.sql.parquet

import scala.collection.mutable.{Buffer, ArrayBuffer}

import parquet.io.api.{PrimitiveConverter, GroupConverter, Binary, Converter}

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row, Attribute}
import org.apache.spark.sql.parquet.CatalystConverter.FieldType

private[parquet] object CatalystConverter {
  // The type internally used for fields
  type FieldType = StructField

  // Note: repeated primitive fields that form an array (together with
  // their surrounding group) need to have this name in the schema
  // TODO: "values" is a generic name but without it the Parquet column path would
  // be incomplete and values may be silently dropped; better would be to give
  // primitive-type array elements a name of some sort
  val ARRAY_ELEMENTS_SCHEMA_NAME = "values"

  protected[parquet] def createConverter(
      field: FieldType,
      fieldIndex: Int,
      parent: CatalystConverter): Converter = {
    val fieldType: DataType = field.dataType
    fieldType match {
      case ArrayType(elementType: DataType) => {
        elementType match {
          case StructType(fields) =>
            if (fields.size > 1) new CatalystGroupConverter(fields, fieldIndex, parent)
            else new CatalystArrayConverter(fields(0).dataType, fieldIndex, parent)
          case _ => new CatalystArrayConverter(elementType, fieldIndex, parent)
        }
      }
      case StructType(fields: Seq[StructField]) => {
        new CatalystStructConverter(fields, fieldIndex, parent)
      }
      case ctype: NativeType => {
        // note: for some reason matching for StringType fails so use this ugly if instead
        if (ctype == StringType) {
          new CatalystPrimitiveStringConverter(parent, fieldIndex)
        } else {
          new CatalystPrimitiveConverter(parent, fieldIndex)
        }
      }
      case _ => throw new RuntimeException(
        s"unable to convert datatype ${field.dataType.toString} in CatalystConverter")
    }
  }
}

trait CatalystConverter {
  // the number of fields this group has
  protected[parquet] val size: Int

  // the index of this converter in the parent
  protected[parquet] val index: Int

  // the parent converter
  protected[parquet] val parent: CatalystConverter

  // for child converters to update upstream values
  protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit

  // TODO: in the future consider using specific methods to avoid autoboxing
  protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit =
    updateField(fieldIndex, value.getBytes)

  protected[parquet] def updateString(fieldIndex: Int, value: Binary): Unit =
    updateField(fieldIndex, value.toStringUsingUTF8)

  protected[parquet] def isRootConverter: Boolean = parent == null

  protected[parquet] def clearBuffer(): Unit
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object.
 *
 * @param schema The corresponding Catalyst schema in the form of a list of attributes.
 */
class CatalystGroupConverter(
    protected[parquet] val schema: Seq[FieldType],
    protected[parquet] val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var current: ArrayBuffer[Any],
    protected[parquet] var buffer: ArrayBuffer[Row])
  extends GroupConverter with CatalystConverter {

  def this(schema: Seq[FieldType], index: Int, parent: CatalystConverter) =
    this(
      schema,
      index,
      parent,
      current=null,
      buffer=new ArrayBuffer[Row](
        CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  // This constructor is used for the root converter only
  def this(attributes: Seq[Attribute]) =
    this(attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)), 0, null)

  protected [parquet] val converters: Array[Converter] =
    schema.map(field =>
      CatalystConverter.createConverter(field, schema.indexOf(field), this))
    .toArray

  override val size = schema.size

  // Should be only called in root group converter!
  def getCurrentRecord: Row = {
    assert(isRootConverter, "getCurrentRecord should only be called in root group converter!")
    // TODO: use iterators if possible
    new GenericRow(current.toArray)
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // for child converters to update upstream values
  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    current.update(fieldIndex, value)
  }

  override protected[parquet] def clearBuffer(): Unit = {
    // TODO: reuse buffer?
    buffer = new ArrayBuffer[Row](CatalystArrayConverter.INITIAL_ARRAY_SIZE)
  }

  override def start(): Unit = {
    // TODO: reuse buffer?
    // Allocate new array in the root converter (others will be called clearBuffer() on)
    current = ArrayBuffer.fill(schema.length)(null)
    converters.foreach {
      converter => if (!converter.isPrimitive) {
        converter.asInstanceOf[CatalystConverter].clearBuffer
      }
    }
  }

  // TODO: think about reusing the buffer
  override def end(): Unit = {
    if (!isRootConverter) {
      assert(current!=null) // there should be no empty groups
      buffer.append(new GenericRow(current.toArray))
      // TODO: use iterators if possible, avoid Row wrapping
      parent.updateField(index, new GenericRow(buffer.toArray.asInstanceOf[Array[Any]]))
    }
  }
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
class CatalystPrimitiveConverter(
    parent: CatalystConverter,
    fieldIndex: Int) extends PrimitiveConverter {
  // TODO: consider refactoring these together with ParquetTypesConverter
  override def addBinary(value: Binary): Unit =
    parent.updateBinary(fieldIndex, value)

  override def addBoolean(value: Boolean): Unit =
    parent.updateBoolean(fieldIndex, value)

  override def addDouble(value: Double): Unit =
    parent.updateDouble(fieldIndex, value)

  override def addFloat(value: Float): Unit =
    parent.updateFloat(fieldIndex, value)

  override def addInt(value: Int): Unit =
    parent.updateInt(fieldIndex, value)

  override def addLong(value: Long): Unit =
    parent.updateLong(fieldIndex, value)
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet strings (fixed-length byte arrays)
 * into Catalyst Strings.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
class CatalystPrimitiveStringConverter(
    parent: CatalystConverter,
    fieldIndex: Int)
  extends CatalystPrimitiveConverter(parent, fieldIndex) {
  override def addBinary(value: Binary): Unit =
    parent.updateString(fieldIndex, value)
}

object CatalystArrayConverter {
  val INITIAL_ARRAY_SIZE = 20
}

// this is for single-element groups of primitive or complex types
// Note: AvroParquet only uses arrays for primitive types (?)
class CatalystArrayConverter(
    val elementType: DataType,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var buffer: Buffer[Any])
  extends GroupConverter with CatalystConverter {
  // TODO: In the future consider using native arrays instead of buffer for
  // primitive types for performance reasons

  def this(elementType: DataType, index: Int, parent: CatalystConverter) =
    this(
      elementType,
      index,
      parent,
      new ArrayBuffer[Any](CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  protected[parquet] val converter: Converter = CatalystConverter.createConverter(
    new CatalystConverter.FieldType(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      elementType,
      false),
    fieldIndex=0,
    parent=this)

  override def getConverter(fieldIndex: Int): Converter = converter

  // arrays have only one (repeated) field, which is its elements
  override val size = 1

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit ={
    buffer += value
  }

  override protected[parquet] def clearBuffer(): Unit = {
    // TODO: reuse buffer?
    buffer = new ArrayBuffer[Any](CatalystArrayConverter.INITIAL_ARRAY_SIZE)
  }

  override def start(): Unit = {
    if (!converter.isPrimitive) {
      converter.asInstanceOf[CatalystConverter].clearBuffer
    }
  }

  // TODO: think about reusing the buffer
  override def end(): Unit = {
    assert(parent != null)
    // TODO: use iterators if possible, avoid Row wrapping
    parent.updateField(index, new GenericRow(buffer.toArray))
    clearBuffer()
  }
}

// this is for multi-element groups of primitive or complex types
// that have repetition level optional or required (so struct fields)
class CatalystStructConverter(
    override protected[parquet] val schema: Seq[FieldType],
    override protected[parquet] val index: Int,
    override protected[parquet] val parent: CatalystConverter)
  extends CatalystGroupConverter(schema, index, parent) {

  override protected[parquet] def clearBuffer(): Unit = {}

  // TODO: think about reusing the buffer
  override def end(): Unit = {
    assert(!isRootConverter)
    // TODO: use iterators if possible, avoid Row wrapping!
    parent.updateField(index, new GenericRow(current.toArray))
  }
}

// TODO: add MapConverter


