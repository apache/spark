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

package org.apache.spark.api.r

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray

/**
 * Utility functions to serialize, deserialize objects to / from R
 */
private[spark] object SerDe {
  type ReadObject = (DataInputStream, Char) => Object
  type WriteObject = (DataOutputStream, Object) => Boolean

  var sqlSerDe: (ReadObject, WriteObject) = _

  def registerSqlSerDe(sqlSerDe: (ReadObject, WriteObject)): Unit = {
    this.sqlSerDe = sqlSerDe
  }

  // Type mapping from R to Java
  //
  // NULL -> void
  // integer -> Int
  // character -> String
  // logical -> Boolean
  // double, numeric -> Double
  // raw -> Array[Byte]
  // Date -> Date
  // POSIXlt/POSIXct -> Time
  //
  // list[T] -> Array[T], where T is one of above mentioned types
  // environment -> Map[String, T], where T is a native type
  // jobj -> Object, where jobj is an object created in the backend

  def readObjectType(dis: DataInputStream): Char = {
    dis.readByte().toChar
  }

  def readObject(dis: DataInputStream): Object = {
    val dataType = readObjectType(dis)
    readTypedObject(dis, dataType)
  }

  def readTypedObject(
      dis: DataInputStream,
      dataType: Char): Object = {
    dataType match {
      case 'n' => null
      case 'i' => new java.lang.Integer(readInt(dis))
      case 'd' => new java.lang.Double(readDouble(dis))
      case 'b' => new java.lang.Boolean(readBoolean(dis))
      case 'c' => readString(dis)
      case 'e' => readMap(dis)
      case 'r' => readBytes(dis)
      case 'a' => readArray(dis)
      case 'l' => readList(dis)
      case 'D' => readDate(dis)
      case 't' => readTime(dis)
      case 'j' => JVMObjectTracker.getObject(readString(dis))
      case _ =>
        if (sqlSerDe == null || sqlSerDe._1 == null) {
          throw new IllegalArgumentException (s"Invalid type $dataType")
        } else {
          val obj = (sqlSerDe._1)(dis, dataType)
          if (obj == null) {
            throw new IllegalArgumentException (s"Invalid type $dataType")
          } else {
            obj
          }
        }
    }
  }

  def readBytes(in: DataInputStream): Array[Byte] = {
    val len = readInt(in)
    val out = new Array[Byte](len)
    val bytesRead = in.readFully(out)
    out
  }

  def readInt(in: DataInputStream): Int = {
    in.readInt()
  }

  def readDouble(in: DataInputStream): Double = {
    in.readDouble()
  }

  def readStringBytes(in: DataInputStream, len: Int): String = {
    val bytes = new Array[Byte](len)
    in.readFully(bytes)
    assert(bytes(len - 1) == 0)
    val str = new String(bytes.dropRight(1), StandardCharsets.UTF_8)
    str
  }

  def readString(in: DataInputStream): String = {
    val len = in.readInt()
    readStringBytes(in, len)
  }

  def readBoolean(in: DataInputStream): Boolean = {
    val intVal = in.readInt()
    if (intVal == 0) false else true
  }

  def readDate(in: DataInputStream): Date = {
    Date.valueOf(readString(in))
  }

  def readTime(in: DataInputStream): Timestamp = {
    val seconds = in.readDouble()
    val sec = Math.floor(seconds).toLong
    val t = new Timestamp(sec * 1000L)
    t.setNanos(((seconds - sec) * 1e9).toInt)
    t
  }

  def readBytesArr(in: DataInputStream): Array[Array[Byte]] = {
    val len = readInt(in)
    (0 until len).map(_ => readBytes(in)).toArray
  }

  def readIntArr(in: DataInputStream): Array[Int] = {
    val len = readInt(in)
    (0 until len).map(_ => readInt(in)).toArray
  }

  def readDoubleArr(in: DataInputStream): Array[Double] = {
    val len = readInt(in)
    (0 until len).map(_ => readDouble(in)).toArray
  }

  def readBooleanArr(in: DataInputStream): Array[Boolean] = {
    val len = readInt(in)
    (0 until len).map(_ => readBoolean(in)).toArray
  }

  def readStringArr(in: DataInputStream): Array[String] = {
    val len = readInt(in)
    (0 until len).map(_ => readString(in)).toArray
  }

  def readZeroLengthArr(in: DataInputStream): Array[Nothing] = {
    val len = readInt(in)
    require(len == 0)
    new Array(0)
  }

  // All elements of an array must be of the same type
  def readArray(dis: DataInputStream): Array[_] = {
    val arrType = readObjectType(dis)
    arrType match {
      case 'n' => readZeroLengthArr(dis)
      case 'i' => readIntArr(dis)
      case 'c' => readStringArr(dis)
      case 'd' => readDoubleArr(dis)
      case 'b' => readBooleanArr(dis)
      case 'j' => readStringArr(dis).map(x => JVMObjectTracker.getObject(x))
      case 'r' => readBytesArr(dis)
      case 'a' =>
        val len = readInt(dis)
        (0 until len).map(_ => readArray(dis)).toArray
      case 'l' =>
        val len = readInt(dis)
        (0 until len).map(_ => readList(dis)).toArray
      case _ =>
        if (sqlSerDe == null || sqlSerDe._1 == null) {
          throw new IllegalArgumentException (s"Invalid array type $arrType")
        } else {
          val len = readInt(dis)
          (0 until len).map { _ =>
            val obj = (sqlSerDe._1)(dis, arrType)
            if (obj == null) {
              throw new IllegalArgumentException (s"Invalid array type $arrType")
            } else {
              obj
            }
          }.toArray
        }
    }
  }

  // Each element of a list can be of different type. They are all represented
  // as Object on JVM side
  def readList(dis: DataInputStream): Array[Object] = {
    val len = readInt(dis)
    (0 until len).map(_ => readObject(dis)).toArray
  }

  def readMap(in: DataInputStream): java.util.Map[Object, Object] = {
    val len = readInt(in)
    if (len > 0) {
      // Keys is an array of String
      val keys = readArray(in).asInstanceOf[Array[Object]]
      val values = readList(in)

      keys.zip(values).toMap.asJava
    } else {
      new java.util.HashMap[Object, Object]()
    }
  }

  // Methods to write out data from Java to R
  //
  // Type mapping from Java to R
  //
  // void -> NULL
  // Int -> integer
  // String -> character
  // Boolean -> logical
  // Float -> double
  // Double -> double
  // Decimal -> double
  // Long -> double
  // Array[Byte] -> raw
  // Date -> Date
  // Time -> POSIXct
  //
  // Array[T] -> list()
  // Object -> jobj

  def writeType(dos: DataOutputStream, typeStr: String): Unit = {
    typeStr match {
      case "void" => dos.writeByte('n')
      case "character" => dos.writeByte('c')
      case "double" => dos.writeByte('d')
      case "integer" => dos.writeByte('i')
      case "logical" => dos.writeByte('b')
      case "date" => dos.writeByte('D')
      case "time" => dos.writeByte('t')
      case "raw" => dos.writeByte('r')
      // Array of primitive types
      case "array" => dos.writeByte('a')
      // Array of objects
      case "list" => dos.writeByte('l')
      case "map" => dos.writeByte('e')
      case "jobj" => dos.writeByte('j')
      case _ => throw new IllegalArgumentException(s"Invalid type $typeStr")
    }
  }

  private def writeKeyValue(dos: DataOutputStream, key: Object, value: Object): Unit = {
    if (key == null) {
      throw new IllegalArgumentException("Key in map can't be null.")
    } else if (!key.isInstanceOf[String]) {
      throw new IllegalArgumentException(s"Invalid map key type: ${key.getClass.getName}")
    }

    writeString(dos, key.asInstanceOf[String])
    writeObject(dos, value)
  }

  def writeObject(dos: DataOutputStream, obj: Object): Unit = {
    if (obj == null) {
      writeType(dos, "void")
    } else {
      // Convert ArrayType collected from DataFrame to Java array
      // Collected data of ArrayType from a DataFrame is observed to be of
      // type "scala.collection.mutable.WrappedArray"
      val value =
        if (obj.isInstanceOf[WrappedArray[_]]) {
          obj.asInstanceOf[WrappedArray[_]].toArray
        } else {
          obj
        }

      value match {
        case v: java.lang.Character =>
          writeType(dos, "character")
          writeString(dos, v.toString)
        case v: java.lang.String =>
          writeType(dos, "character")
          writeString(dos, v)
        case v: java.lang.Long =>
          writeType(dos, "double")
          writeDouble(dos, v.toDouble)
        case v: java.lang.Float =>
          writeType(dos, "double")
          writeDouble(dos, v.toDouble)
        case v: java.math.BigDecimal =>
          writeType(dos, "double")
          writeDouble(dos, scala.math.BigDecimal(v).toDouble)
        case v: java.lang.Double =>
          writeType(dos, "double")
          writeDouble(dos, v)
        case v: java.lang.Byte =>
          writeType(dos, "integer")
          writeInt(dos, v.toInt)
        case v: java.lang.Short =>
          writeType(dos, "integer")
          writeInt(dos, v.toInt)
        case v: java.lang.Integer =>
          writeType(dos, "integer")
          writeInt(dos, v)
        case v: java.lang.Boolean =>
          writeType(dos, "logical")
          writeBoolean(dos, v)
        case v: java.sql.Date =>
          writeType(dos, "date")
          writeDate(dos, v)
        case v: java.sql.Time =>
          writeType(dos, "time")
          writeTime(dos, v)
        case v: java.sql.Timestamp =>
          writeType(dos, "time")
          writeTime(dos, v)

        // Handle arrays

        // Array of primitive types

        // Special handling for byte array
        case v: Array[Byte] =>
          writeType(dos, "raw")
          writeBytes(dos, v)

        case v: Array[Char] =>
          writeType(dos, "array")
          writeStringArr(dos, v.map(_.toString))
        case v: Array[Short] =>
          writeType(dos, "array")
          writeIntArr(dos, v.map(_.toInt))
        case v: Array[Int] =>
          writeType(dos, "array")
          writeIntArr(dos, v)
        case v: Array[Long] =>
          writeType(dos, "array")
          writeDoubleArr(dos, v.map(_.toDouble))
        case v: Array[Float] =>
          writeType(dos, "array")
          writeDoubleArr(dos, v.map(_.toDouble))
        case v: Array[Double] =>
          writeType(dos, "array")
          writeDoubleArr(dos, v)
        case v: Array[Boolean] =>
          writeType(dos, "array")
          writeBooleanArr(dos, v)

        // Array of objects, null objects use "void" type
        case v: Array[Object] =>
          writeType(dos, "list")
          writeInt(dos, v.length)
          v.foreach(elem => writeObject(dos, elem))

        // Handle Properties
        // This must be above the case java.util.Map below.
        // (Properties implements Map<Object,Object> and will be serialized as map otherwise)
        case v: java.util.Properties =>
          writeType(dos, "jobj")
          writeJObj(dos, value)

        // Handle map
        case v: java.util.Map[_, _] =>
          writeType(dos, "map")
          writeInt(dos, v.size)
          val iter = v.entrySet.iterator
          while(iter.hasNext) {
            val entry = iter.next
            val key = entry.getKey
            val value = entry.getValue

            writeKeyValue(dos, key.asInstanceOf[Object], value.asInstanceOf[Object])
          }
        case v: scala.collection.Map[_, _] =>
          writeType(dos, "map")
          writeInt(dos, v.size)
          v.foreach { case (key, value) =>
            writeKeyValue(dos, key.asInstanceOf[Object], value.asInstanceOf[Object])
          }

        case _ =>
          if (sqlSerDe == null || sqlSerDe._2 == null || !(sqlSerDe._2)(dos, value)) {
            writeType(dos, "jobj")
            writeJObj(dos, value)
          }
      }
    }
  }

  def writeInt(out: DataOutputStream, value: Int): Unit = {
    out.writeInt(value)
  }

  def writeDouble(out: DataOutputStream, value: Double): Unit = {
    out.writeDouble(value)
  }

  def writeBoolean(out: DataOutputStream, value: Boolean): Unit = {
    val intValue = if (value) 1 else 0
    out.writeInt(intValue)
  }

  def writeDate(out: DataOutputStream, value: Date): Unit = {
    writeString(out, value.toString)
  }

  def writeTime(out: DataOutputStream, value: Time): Unit = {
    out.writeDouble(value.getTime.toDouble / 1000.0)
  }

  def writeTime(out: DataOutputStream, value: Timestamp): Unit = {
    out.writeDouble((value.getTime / 1000).toDouble + value.getNanos.toDouble / 1e9)
  }

  def writeString(out: DataOutputStream, value: String): Unit = {
    val utf8 = value.getBytes(StandardCharsets.UTF_8)
    val len = utf8.length
    out.writeInt(len)
    out.write(utf8, 0, len)
  }

  def writeBytes(out: DataOutputStream, value: Array[Byte]): Unit = {
    out.writeInt(value.length)
    out.write(value)
  }

  def writeJObj(out: DataOutputStream, value: Object): Unit = {
    val objId = JVMObjectTracker.put(value)
    writeString(out, objId)
  }

  def writeIntArr(out: DataOutputStream, value: Array[Int]): Unit = {
    writeType(out, "integer")
    out.writeInt(value.length)
    value.foreach(v => out.writeInt(v))
  }

  def writeDoubleArr(out: DataOutputStream, value: Array[Double]): Unit = {
    writeType(out, "double")
    out.writeInt(value.length)
    value.foreach(v => out.writeDouble(v))
  }

  def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]): Unit = {
    writeType(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeBoolean(out, v))
  }

  def writeStringArr(out: DataOutputStream, value: Array[String]): Unit = {
    writeType(out, "character")
    out.writeInt(value.length)
    value.foreach(v => writeString(out, v))
  }

}

private[spark] object SerializationFormats {
  val BYTE = "byte"
  val STRING = "string"
  val ROW = "row"
}
