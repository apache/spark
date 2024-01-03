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
package org.apache.spark.sql.protobuf

import java.util.concurrent.TimeUnit

import com.google.protobuf.{BoolValue, ByteString, BytesValue, DoubleValue, DynamicMessage, FloatValue, Int32Value, Int64Value, Message, StringValue, TypeRegistry, UInt32Value, UInt64Value}
import com.google.protobuf.Descriptors._
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.util.JsonFormat

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.utils.ProtobufUtils.ProtoMatchedField
import org.apache.spark.sql.protobuf.utils.ProtobufUtils.toFieldStr
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] class ProtobufDeserializer(
    rootDescriptor: Descriptor,
    rootCatalystType: DataType,
    filters: StructFilters = new NoopFilters,
    typeRegistry: TypeRegistry = TypeRegistry.getEmptyTypeRegistry,
    emitDefaultValues: Boolean = false,
    enumsAsInts: Boolean = false) {

  def this(rootDescriptor: Descriptor, rootCatalystType: DataType) = {
    this(
      rootDescriptor, rootCatalystType, new NoopFilters, TypeRegistry.getEmptyTypeRegistry, false
    )
  }

  private val converter: Any => Option[InternalRow] =
    try {
      rootCatalystType match {
        // A shortcut for empty schema.
        case st: StructType if st.isEmpty =>
          (_: Any) => Some(InternalRow.empty)

        case st: StructType =>
          val resultRow = new SpecificInternalRow(st.map(_.dataType))
          val fieldUpdater = new RowUpdater(resultRow)
          val applyFilters = filters.skipRow(resultRow, _)
          val writer = getRecordWriter(rootDescriptor, st, Nil, Nil, applyFilters)
          (data: Any) => {
            val record = data.asInstanceOf[DynamicMessage]
            val skipRow = writer(fieldUpdater, record)
            if (skipRow) None else Some(resultRow)
          }
      }
    } catch {
      case ise: AnalysisException =>
        throw QueryCompilationErrors.cannotConvertProtobufTypeToCatalystTypeError(
          rootDescriptor.getName,
          rootCatalystType,
          ise)
    }

  def deserialize(data: Message): Option[InternalRow] = converter(data)

  // JsonFormatter used to convert Any fields (if the option is enabled).
  // This keeps original field names and does not include any extra whitespace in JSON.
  // If the runtime type for Any field is not found in the registry, it throws an exception.
  private val jsonPrinter = if (enumsAsInts) {
    JsonFormat.printer
      .omittingInsignificantWhitespace()
      .preservingProtoFieldNames()
      .printingEnumsAsInts()
      .usingTypeRegistry(typeRegistry)
  } else {
    JsonFormat.printer
      .omittingInsignificantWhitespace()
      .preservingProtoFieldNames()
      .usingTypeRegistry(typeRegistry)
  }

  private def newArrayWriter(
      protoField: FieldDescriptor,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      elementType: DataType,
      containsNull: Boolean): (CatalystDataUpdater, Int, Any) => Unit = {

    val protoElementPath = protoPath :+ "element"
    val elementWriter =
      newWriter(protoField, elementType, protoElementPath, catalystPath :+ "element")
    (updater, ordinal, value) =>
      val collection = value.asInstanceOf[java.util.Collection[Any]]
      val result = createArrayData(elementType, collection.size())
      val elementUpdater = new ArrayDataUpdater(result)

      var i = 0
      val iterator = collection.iterator()
      while (iterator.hasNext) {
        val element = iterator.next()
        if (element == null) {
          if (!containsNull) {
            throw QueryCompilationErrors.notNullConstraintViolationArrayElementError(
              protoElementPath)
          } else {
            elementUpdater.setNullAt(i)
          }
        } else {
          elementWriter(elementUpdater, i, element)
        }
        i += 1
      }

      updater.set(ordinal, result)
  }

  private def newMapWriter(
      protoType: FieldDescriptor,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean): (CatalystDataUpdater, Int, Any) => Unit = {
    val keyField = protoType.getMessageType.getFields.get(0)
    val valueField = protoType.getMessageType.getFields.get(1)
    val keyWriter = newWriter(keyField, keyType, protoPath :+ "key", catalystPath :+ "key")
    val valueWriter =
      newWriter(valueField, valueType, protoPath :+ "value", catalystPath :+ "value")
    (updater, ordinal, value) =>
      if (value != null) {
        val messageList = value.asInstanceOf[java.util.List[com.google.protobuf.Message]]
        val valueArray = createArrayData(valueType, messageList.size())
        val valueUpdater = new ArrayDataUpdater(valueArray)
        val keyArray = createArrayData(keyType, messageList.size())
        val keyUpdater = new ArrayDataUpdater(keyArray)
        var i = 0
        messageList.forEach { field =>
          {
            keyWriter(keyUpdater, i, field.getField(keyField))
            if (field.getField(valueField) == null) {
              if (!valueContainsNull) {
                throw QueryCompilationErrors.notNullConstraintViolationMapValueError(protoPath)
              } else {
                valueUpdater.setNullAt(i)
              }
            } else {
              valueWriter(valueUpdater, i, field.getField(valueField))
            }
          }
          i += 1
        }
        updater.set(ordinal, new ArrayBasedMapData(keyArray, valueArray))
      }
  }

  /**
   * Creates a writer to write Protobuf values to Catalyst values at the given ordinal with the
   * given updater.
   */
  private def newWriter(
      protoType: FieldDescriptor,
      catalystType: DataType,
      protoPath: Seq[String],
      catalystPath: Seq[String]): (CatalystDataUpdater, Int, Any) => Unit = {

    (protoType.getJavaType, catalystType) match {

      case (null, NullType) => (updater, ordinal, _) => updater.setNullAt(ordinal)

      // TODO: we can avoid boxing if future version of Protobuf provide primitive accessors.
      case (BOOLEAN, BooleanType) =>
        (updater, ordinal, value) => updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (INT, IntegerType) =>
        (updater, ordinal, value) => updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, ByteType) =>
        (updater, ordinal, value) => updater.setByte(ordinal, value.asInstanceOf[Byte])

      case (INT, ShortType) =>
        (updater, ordinal, value) => updater.setShort(ordinal, value.asInstanceOf[Short])

      case (INT, LongType) =>
        (updater, ordinal, value) =>
          updater.setLong(
            ordinal,
            Integer.toUnsignedLong(value.asInstanceOf[Int]))
      case  (
        MESSAGE | BOOLEAN | INT | FLOAT | DOUBLE | LONG | STRING | ENUM | BYTE_STRING,
        ArrayType(dataType: DataType, containsNull)) if protoType.isRepeated =>
        newArrayWriter(protoType, protoPath, catalystPath, dataType, containsNull)

      case (LONG, LongType) =>
        (updater, ordinal, value) => updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, DecimalType.LongDecimal) =>
        (updater, ordinal, value) =>
          updater.setDecimal(
            ordinal,
            Decimal.fromString(
              UTF8String.fromString(java.lang.Long.toUnsignedString(value.asInstanceOf[Long]))))

      case (FLOAT, FloatType) =>
        (updater, ordinal, value) => updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (DOUBLE, DoubleType) =>
        (updater, ordinal, value) => updater.setDouble(ordinal, value.asInstanceOf[Double])

      case (STRING, StringType) =>
        (updater, ordinal, value) =>
          val str = value match {
            case s: String => UTF8String.fromString(s)
          }
          updater.set(ordinal, str)

      case (BYTE_STRING, BinaryType) =>
        (updater, ordinal, value) =>
          val byte_array = value match {
            case s: ByteString => s.toByteArray
            case unsupported =>
              throw QueryCompilationErrors.invalidByteStringFormatError(unsupported)
          }
          updater.set(ordinal, byte_array)

      case (MESSAGE, MapType(keyType, valueType, valueContainsNull)) =>
        newMapWriter(protoType, protoPath, catalystPath, keyType, valueType, valueContainsNull)

      case (MESSAGE, TimestampType) =>
        (updater, ordinal, value) =>
          val secondsField = protoType.getMessageType.getFields.get(0)
          val nanoSecondsField = protoType.getMessageType.getFields.get(1)
          val message = value.asInstanceOf[DynamicMessage]
          val seconds = message.getField(secondsField).asInstanceOf[Long]
          val nanoSeconds = message.getField(nanoSecondsField).asInstanceOf[Int]
          val micros = DateTimeUtils.millisToMicros(seconds * 1000)
          updater.setLong(ordinal, micros + TimeUnit.NANOSECONDS.toMicros(nanoSeconds))

      case (MESSAGE, DayTimeIntervalType(startField, endField)) =>
        (updater, ordinal, value) =>
          val secondsField = protoType.getMessageType.getFields.get(0)
          val nanoSecondsField = protoType.getMessageType.getFields.get(1)
          val message = value.asInstanceOf[DynamicMessage]
          val seconds = message.getField(secondsField).asInstanceOf[Long]
          val nanoSeconds = message.getField(nanoSecondsField).asInstanceOf[Int]
          val micros = DateTimeUtils.millisToMicros(seconds * 1000)
          updater.setLong(ordinal, micros + TimeUnit.NANOSECONDS.toMicros(nanoSeconds))

      case (MESSAGE, StringType)
        if protoType.getMessageType.getFullName == "google.protobuf.Any" =>
        (updater, ordinal, value) =>
          // Convert 'Any' protobuf message to JSON string.
          val jsonStr = jsonPrinter.print(value.asInstanceOf[DynamicMessage])
          updater.set(ordinal, UTF8String.fromString(jsonStr))

      // Handle well known wrapper types. We unpack the value field when the desired
      // output type is a primitive (determined by the option in [[ProtobufOptions]])
      case (MESSAGE, BooleanType)
        if protoType.getMessageType.getFullName == BoolValue.getDescriptor.getFullName =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.setBoolean(ordinal, unwrapped.asInstanceOf[Boolean])
          }
      case (MESSAGE, IntegerType)
        if (protoType.getMessageType.getFullName == Int32Value.getDescriptor.getFullName
          || protoType.getMessageType.getFullName == UInt32Value.getDescriptor.getFullName) =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.setInt(ordinal, unwrapped.asInstanceOf[Int])
          }
      case (MESSAGE, LongType)
        if (protoType.getMessageType.getFullName == UInt32Value.getDescriptor.getFullName) =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.setLong(ordinal, Integer.toUnsignedLong(unwrapped.asInstanceOf[Int]))
          }
      case (MESSAGE, LongType)
        if (protoType.getMessageType.getFullName == Int64Value.getDescriptor.getFullName
          || protoType.getMessageType.getFullName == UInt64Value.getDescriptor.getFullName) =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.setLong(ordinal, unwrapped.asInstanceOf[Long])
          }
      case (MESSAGE, DecimalType.LongDecimal)
        if (protoType.getMessageType.getFullName == UInt64Value.getDescriptor.getFullName) =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            val dec = Decimal.fromString(
              UTF8String.fromString(java.lang.Long.toUnsignedString(unwrapped.asInstanceOf[Long])))
            updater.setDecimal(ordinal, dec)
          }
      case (MESSAGE, StringType)
        if protoType.getMessageType.getFullName == StringValue.getDescriptor.getFullName =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.set(ordinal, UTF8String.fromString(unwrapped.asInstanceOf[String]))
          }
      case (MESSAGE, BinaryType)
        if protoType.getMessageType.getFullName == BytesValue.getDescriptor.getFullName =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.set(ordinal, unwrapped.asInstanceOf[ByteString].toByteArray)
          }
      case (MESSAGE, FloatType)
        if protoType.getMessageType.getFullName == FloatValue.getDescriptor.getFullName =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.setFloat(ordinal, unwrapped.asInstanceOf[Float])
          }
      case (MESSAGE, DoubleType)
        if protoType.getMessageType.getFullName == DoubleValue.getDescriptor.getFullName =>
        (updater, ordinal, value) =>
          val dm = value.asInstanceOf[DynamicMessage]
          val unwrapped = getFieldValue(dm, dm.getDescriptorForType.getFields.get(0))
          if (unwrapped == null) {
            updater.setNullAt(ordinal)
          } else {
            updater.setDouble(ordinal, unwrapped.asInstanceOf[Double])
          }

      case (MESSAGE, st: StructType) =>
        val writeRecord = getRecordWriter(
          protoType.getMessageType,
          st,
          protoPath,
          catalystPath,
          applyFilters = _ => false)
        (updater, ordinal, value) =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[DynamicMessage])
          updater.set(ordinal, row)

      case (ENUM, StringType) =>
        (updater, ordinal, value) =>
          updater.set(
            ordinal,
            UTF8String.fromString(value.asInstanceOf[EnumValueDescriptor].getName))

      case (ENUM, IntegerType) =>
        (updater, ordinal, value) =>
          updater.setInt(ordinal, value.asInstanceOf[EnumValueDescriptor].getNumber)

      case _ =>
        throw QueryCompilationErrors.cannotConvertProtobufTypeToSqlTypeError(
          toFieldStr(protoPath),
          catalystPath,
          s"${protoType} ${protoType.toProto.getLabel} ${protoType.getJavaType}" +
            s" ${protoType.getType}",
          catalystType)
    }
  }

  private def getRecordWriter(
      protoType: Descriptor,
      catalystType: StructType,
      protoPath: Seq[String],
      catalystPath: Seq[String],
      applyFilters: Int => Boolean): (CatalystDataUpdater, DynamicMessage) => Boolean = {

    val protoSchemaHelper =
      new ProtobufUtils.ProtoSchemaHelper(protoType, catalystType, protoPath, catalystPath)

    // TODO revisit validation of protobuf-catalyst fields.
    // protoSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = true)

    var i = 0
    val (validFieldIndexes, fieldWriters) = protoSchemaHelper.matchedFields
      .map { case ProtoMatchedField(catalystField, ordinal, protoField) =>
        val baseWriter = newWriter(
          protoField,
          catalystField.dataType,
          protoPath :+ protoField.getName,
          catalystPath :+ catalystField.name)
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        i += 1
        (protoField, fieldWriter)
      }
      .toArray
      .unzip

    (fieldUpdater, record) => {
      var i = 0
      var skipRow = false
      while (i < validFieldIndexes.length && !skipRow) {
        val field = validFieldIndexes(i)
        val value = getFieldValue(record, field)
        fieldWriters(i)(fieldUpdater, value)
        skipRow = applyFilters(i)
        i += 1
      }
      skipRow
    }
  }

  private def getFieldValue(record: DynamicMessage, field: FieldDescriptor): AnyRef = {
    // We return a value if one of:
    // - the field is repeated
    // - the field is explicitly present in the serialized proto
    // - the field is proto2 with a default
    // - emitDefaultValues is set, and the field type is one where default values
    //   are not present in the wire format. This includes singular proto3 scalars,
    //   but not messages / oneof / proto2.
    //   See [[ProtobufOptions]] and https://protobuf.dev/programming-guides/field_presence
    //   for more information.
    //
    // Repeated fields have to be treated separately as they cannot have `hasField`
    // called on them.
    if (
      field.isRepeated
        || record.hasField(field)
        || field.hasDefaultValue
        || (!field.hasPresence && this.emitDefaultValues)
    ) {
      record.getField(field)
    } else {
      null
    }
  }

  // TODO: All of the code below this line is same between protobuf and avro, it can be shared.
  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  /**
   * A base interface for updating values inside catalyst data structure like `InternalRow` and
   * `ArrayData`.
   */
  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit
    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }

}
