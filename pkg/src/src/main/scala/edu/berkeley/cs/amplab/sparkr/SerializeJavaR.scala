package edu.berkeley.cs.amplab.sparkr

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import java.io.DataInputStream
import java.io.DataOutputStream

object SerializeJavaR {

  def readObject(dis: DataInputStream, objMap: HashMap[String, Object]): Object = {
    val dataType = readString(dis)
    println("reading object of type " + dataType)
    readObjectType(dis, objMap, dataType)
  }

  def readObjectType(
      dis: DataInputStream,
      objMap: HashMap[String, Object],
      dataType: String): Object = {
    dataType match {
      case "integer" => new java.lang.Integer(readInt(dis))
      case "double" => new java.lang.Double(readDouble(dis))
      case "logical" => new java.lang.Boolean(readBoolean(dis))
      case "character" => readString(dis)
      case "environment" => readMap(dis, objMap)
      case "raw" => readBytes(dis)
      case "list" => {
        val arrType = readString(dis)
        arrType match {
          case "integer" => readIntArr(dis)
          case "character" => readStringArr(dis)
          case "double" => readDoubleArr(dis)
          case "logical" => readBooleanArr(dis)
          case "jobj" => readStringArr(dis).map(x => objMap(x))
          case "raw" => readBytesArr(dis)
        }
      }
      case "jobj" => objMap(readString(dis))
    }
  }

  def readBytes(in: DataInputStream) = {
    val len = readInt(in)
    val out = new Array[Byte](len)
    val bytesRead = in.read(out, 0, len)
    assert(len == bytesRead)
    out
  }

  def readInt(in: DataInputStream) = {
    in.readInt()
  }

  def readDouble(in: DataInputStream) = {
    in.readDouble()
  }

  def readString(in: DataInputStream) = {
    val len = in.readInt()
    val asciiBytes = new Array[Byte](len)
    in.read(asciiBytes, 0, len)
    new String(asciiBytes.dropRight(1).map(_.toChar))
  }

  def readBoolean(in: DataInputStream) = {
    val intVal = in.readInt()
    if (intVal == 0) false else true
  }

  def readBytesArr(in: DataInputStream) = {
    val len = readInt(in)
    (0 until len).map(_ => readBytes(in)).toArray
  }

  def readIntArr(in: DataInputStream) = {
    val len = readInt(in)
    (0 until len).map(_ => readInt(in)).toArray
  }

  def readDoubleArr(in: DataInputStream) = {
    val len = readInt(in)
    (0 until len).map(_ => readDouble(in)).toArray
  }

  def readBooleanArr(in: DataInputStream) = {
    val len = readInt(in)
    (0 until len).map(_ => readBoolean(in)).toArray
  }

  def readStringArr(in: DataInputStream) = {
    val len = readInt(in)
    (0 until len).map(_ => readString(in)).toArray
  }

  def readMap(
      in: DataInputStream,
      objMap: HashMap[String, Object]): java.util.Map[Object, Object] = {
    val len = readInt(in)
    if (len > 0) {
      val typeStr = readString(in)
      val keys = (0 until len).map(_ => readObjectType(in, objMap, typeStr))
      val valuesType = readString(in)
      val values = (0 until len).map(_ => readObjectType(in, objMap, typeStr))
      mapAsJavaMap(keys.zip(values).toMap)
    } else {
      new java.util.HashMap[Object, Object]()
    }
  }

  /// Methods to write out data from Java to R

  def writeObject(dos: DataOutputStream, value: Object, objMap: HashMap[String, Object]) {
    value.getClass.getName match {
      case "void" => {
        writeString(dos, "void")
      }
      case "java.lang.String" => {
        writeString(dos, "character")
        writeString(dos, value.asInstanceOf[String])
      }
      case "long" | "java.lang.Long" => {
        writeString(dos, "double")
        writeDouble(dos, value.asInstanceOf[Long].toDouble)
      }
      case "double" | "java.lang.Double" => {
        writeString(dos, "double")
        writeDouble(dos, value.asInstanceOf[Double])
      }
      case "int" | "java.lang.Integer" => {
        writeString(dos, "integer")
        writeInt(dos, value.asInstanceOf[Int])
      }
      case "boolean" | "java.lang.Boolean" => {
        writeString(dos, "logical")
        writeBoolean(dos, value.asInstanceOf[Boolean])
      }
      case "[B" => {
        writeString(dos, "raw")
        writeBytes(dos, value.asInstanceOf[Array[Byte]])
      }
      // TODO: Types not handled right now include
      // byte, char, short, float

      // Handle arrays
      case "[Ljava.lang.String;" => {
        writeString(dos, "list")
        writeStringArr(dos, value.asInstanceOf[Array[String]])
      }
      case "[I" => {
        writeString(dos, "list")
        writeIntArr(dos, value.asInstanceOf[Array[Int]])
      }
      case "[L" => {
        writeString(dos, "list")
        writeDoubleArr(dos, value.asInstanceOf[Array[Long]].map(_.toDouble))
      }
      case "[D" => {
        writeString(dos, "list")
        writeDoubleArr(dos, value.asInstanceOf[Array[Double]])
      }
      case "[Z" => {
        writeString(dos, "list")
        writeBooleanArr(dos, value.asInstanceOf[Array[Boolean]])
      }
      case "[[B" => {
        writeString(dos, "list")
        writeBytesArr(dos, value.asInstanceOf[Array[Array[Byte]]])
      }
      case _ => {
        val objId = value.getClass().getName() + "@" +
          Integer.toHexString(System.identityHashCode(value))
        objMap.put(objId, value)
        writeString(dos, "jobj")
        writeString(dos, objId)
      }
    }
  }

  def writeInt(out: DataOutputStream, value: Int) {
    out.writeInt(value)
  }

  def writeDouble(out: DataOutputStream, value: Double) {
    out.writeDouble(value)
  }

  def writeBoolean(out: DataOutputStream, value: Boolean) {
    val intValue = if (value) 1 else 0
    out.writeInt(intValue)
  }

  // NOTE: Only works for ASCII right now
  def writeString(out: DataOutputStream, value: String) {
    val len = value.length
    out.writeInt(len + 1) // For the \0
    out.writeBytes(value)
    out.writeByte(0)
  }

  def writeBytes(out: DataOutputStream, value: Array[Byte]) {
    out.writeInt(value.length)
    out.write(value)
  }

  def writeIntArr(out: DataOutputStream, value: Array[Int]) {
    writeString(out, "integer")
    out.writeInt(value.length)
    value.foreach(v => out.writeInt(v))
  }

  def writeDoubleArr(out: DataOutputStream, value: Array[Double]) {
    writeString(out, "double")
    out.writeInt(value.length)
    value.foreach(v => out.writeDouble(v))
  }

  def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]) {
    writeString(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeBoolean(out, v))
  }

  def writeStringArr(out: DataOutputStream, value: Array[String]) {
    writeString(out, "character")
    out.writeInt(value.length)
    value.foreach(v => writeString(out, v))
  }

  def writeBytesArr(out: DataOutputStream, value: Array[Array[Byte]]) {
    writeString(out, "raw")
    out.writeInt(value.length)
    value.foreach(v => writeBytes(out, v))
  }
}
