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

import org.apache.spark.sql.types._

/**
 * A parent class for mutable container objects that are reused when the values are changed,
 * resulting in less garbage.  These values are held by a [[SpecificInternalRow]].
 *
 * The following code was roughly used to generate these objects:
 * {{{
 * val types = "Int,Float,Boolean,Double,Short,Long,Byte,Any".split(",")
 * types.map {tpe =>
 * s"""
 * final class Mutable$tpe extends MutableValue {
 *   var value: $tpe = 0
 *   def boxed = if (isNull) null else value
 *   def update(v: Any) = value = {
 *     isNull = false
 *     v.asInstanceOf[$tpe]
 *   }
 *   def copy() = {
 *     val newCopy = new Mutable$tpe
 *     newCopy.isNull = isNull
 *     newCopy.value = value
 *     newCopy
 *   }
 * }"""
 * }.foreach(println)
 *
 * types.map { tpe =>
 * s"""
 *   override def set$tpe(ordinal: Int, value: $tpe): Unit = {
 *     val currentValue = values(ordinal).asInstanceOf[Mutable$tpe]
 *     currentValue.isNull = false
 *     currentValue.value = value
 *   }
 *
 *   override def get$tpe(i: Int): $tpe = {
 *     values(i).asInstanceOf[Mutable$tpe].value
 *   }"""
 * }.foreach(println)
 * }}}
 */
abstract class MutableValue extends Serializable {
  var isNull: Boolean = true
  def boxed: Any
  def update(v: Any): Unit
  def copy(): MutableValue
}

final class MutableInt extends MutableValue {
  var value: Int = 0
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = {
    isNull = false
    value = v.asInstanceOf[Int]
  }
  override def copy(): MutableInt = {
    val newCopy = new MutableInt
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableFloat extends MutableValue {
  var value: Float = 0
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = {
    isNull = false
    value = v.asInstanceOf[Float]
  }
  override def copy(): MutableFloat = {
    val newCopy = new MutableFloat
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableBoolean extends MutableValue {
  var value: Boolean = false
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = {
    isNull = false
    value = v.asInstanceOf[Boolean]
  }
  override def copy(): MutableBoolean = {
    val newCopy = new MutableBoolean
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableDouble extends MutableValue {
  var value: Double = 0
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = {
    isNull = false
    value = v.asInstanceOf[Double]
  }
  override def copy(): MutableDouble = {
    val newCopy = new MutableDouble
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableShort extends MutableValue {
  var value: Short = 0
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = value = {
    isNull = false
    v.asInstanceOf[Short]
  }
  override def copy(): MutableShort = {
    val newCopy = new MutableShort
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableLong extends MutableValue {
  var value: Long = 0
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = value = {
    isNull = false
    v.asInstanceOf[Long]
  }
  override def copy(): MutableLong = {
    val newCopy = new MutableLong
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableByte extends MutableValue {
  var value: Byte = 0
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = value = {
    isNull = false
    v.asInstanceOf[Byte]
  }
  override def copy(): MutableByte = {
    val newCopy = new MutableByte
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

final class MutableAny extends MutableValue {
  var value: Any = _
  override def boxed: Any = if (isNull) null else value
  override def update(v: Any): Unit = {
    isNull = false
    value = v
  }
  override def copy(): MutableAny = {
    val newCopy = new MutableAny
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

/**
 * A row type that holds an array specialized container objects, of type [[MutableValue]], chosen
 * based on the dataTypes of each column.  The intent is to decrease garbage when modifying the
 * values of primitive columns.
 */
final class SpecificInternalRow(val values: Array[MutableValue]) extends BaseGenericInternalRow {

  private[this] def dataTypeToMutableValue(dataType: DataType): MutableValue = dataType match {
    // We use INT for DATE and YearMonthIntervalType internally
    case IntegerType | DateType | _: YearMonthIntervalType => new MutableInt
    // We use Long for Timestamp, Timestamp without time zone and DayTimeInterval internally
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType | _: TimeType =>
      new MutableLong
    case FloatType => new MutableFloat
    case DoubleType => new MutableDouble
    case BooleanType => new MutableBoolean
    case ByteType => new MutableByte
    case ShortType => new MutableShort
    case _ => new MutableAny
  }

  def this(dataTypes: Seq[DataType]) = {
    // SPARK-32550: use `dataTypes.foreach` instead of `while loop + dataTypes(i)` to ensure
    // constant-time access of dataTypes `Seq` because it is not necessarily an `IndexSeq` that
    // support constant-time access.
    this(new Array[MutableValue](dataTypes.length))
    var i = 0
    dataTypes.foreach { dt =>
      values(i) = dataTypeToMutableValue(dt)
      i += 1
    }
  }

  def this() = this(Seq.empty)

  def this(schema: StructType) = {
    // SPARK-32550: use while loop instead of map
    this(new Array[MutableValue](schema.fields.length))
    val length = values.length
    val fields = schema.fields
    var i = 0
    while (i < length) {
      values(i) = dataTypeToMutableValue(fields(i).dataType)
      i += 1
    }
  }

  override def numFields: Int = values.length

  override def setNullAt(i: Int): Unit = {
    values(i).isNull = true
  }

  override def isNullAt(i: Int): Boolean = values(i).isNull

  override protected def genericGet(i: Int): Any = values(i).boxed

  override def update(ordinal: Int, value: Any): Unit = {
    if (value == null) {
      setNullAt(ordinal)
    } else {
      values(ordinal).update(value)
    }
  }

  override def setInt(ordinal: Int, value: Int): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableInt]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getInt(i: Int): Int = {
    values(i).asInstanceOf[MutableInt].value
  }

  override def setFloat(ordinal: Int, value: Float): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableFloat]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getFloat(i: Int): Float = {
    values(i).asInstanceOf[MutableFloat].value
  }

  override def setBoolean(ordinal: Int, value: Boolean): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableBoolean]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getBoolean(i: Int): Boolean = {
    values(i).asInstanceOf[MutableBoolean].value
  }

  override def setDouble(ordinal: Int, value: Double): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableDouble]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getDouble(i: Int): Double = {
    values(i).asInstanceOf[MutableDouble].value
  }

  override def setShort(ordinal: Int, value: Short): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableShort]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getShort(i: Int): Short = {
    values(i).asInstanceOf[MutableShort].value
  }

  override def setLong(ordinal: Int, value: Long): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableLong]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getLong(i: Int): Long = {
    values(i).asInstanceOf[MutableLong].value
  }

  override def setByte(ordinal: Int, value: Byte): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableByte]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getByte(i: Int): Byte = {
    values(i).asInstanceOf[MutableByte].value
  }
}
