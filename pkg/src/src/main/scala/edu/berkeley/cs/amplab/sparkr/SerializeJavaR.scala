package edu.berkeley.cs.amplab.sparkr

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import java.io.DataInputStream
import java.io.DataOutputStream

object SerializeJavaR {
  // We supports only one connection now, so no need to use atomic integer.
  var objCounter: Int = 0

  def readObjectType(dis: DataInputStream) = {
    dis.readByte().toChar
  }

  def readObject(dis: DataInputStream, objMap: HashMap[String, Object]): Object = {
    val dataType = readObjectType(dis)
    readTypedObject(dis, objMap, dataType)
  }

  def readTypedObject(
      dis: DataInputStream,
      objMap: HashMap[String, Object],
      dataType: Char): Object = {
    dataType match {
      case 'i' => new java.lang.Integer(readInt(dis))
      case 'd' => new java.lang.Double(readDouble(dis))
      case 'b' => new java.lang.Boolean(readBoolean(dis))
      case 'c' => readString(dis)
      case 'e' => readMap(dis, objMap)
      case 'r' => readBytes(dis)
      case 'l' => readList(dis, objMap)
      case 'j' => objMap(readString(dis))
      case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
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
    val str = new String(asciiBytes.dropRight(1).map(_.toChar))
    str
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

  def readList(dis: DataInputStream, objMap: HashMap[String, Object]) = {
    val arrType = readObjectType(dis)
    arrType match {
      case 'i' => readIntArr(dis)
      case 'c' => readStringArr(dis)
      case 'd' => readDoubleArr(dis)
      case 'l' => readBooleanArr(dis)
      case 'j' => readStringArr(dis).map(x => objMap(x))
      case 'r' => readBytesArr(dis)
      case _ => throw new IllegalArgumentException(s"Invalid array type $arrType")
    }
  }

  def readMap(
      in: DataInputStream,
      objMap: HashMap[String, Object]): java.util.Map[Object, Object] = {
    val len = readInt(in)
    if (len > 0) {
      val keysType = readObjectType(in)
      val keysLen = readInt(in)
      val keys = (0 until keysLen).map(_ => readTypedObject(in, objMap, keysType))

      val valuesType = readObjectType(in)
      val valuesLen = readInt(in)
      val values = (0 until len).map(_ => readTypedObject(in, objMap, valuesType))
      mapAsJavaMap(keys.zip(values).toMap)
    } else {
      new java.util.HashMap[Object, Object]()
    }
  }

  /// Methods to write out data from Java to R

  def writeType(dos: DataOutputStream, typeStr: String) {
    typeStr match {
      case "void" => dos.writeByte('n')
      case "character" => dos.writeByte('c')
      case "double" => dos.writeByte('d')
      case "integer" => dos.writeByte('i')
      case "logical" => dos.writeByte('b')
      case "raw" => dos.writeByte('r')
      case "list" => dos.writeByte('l')
      case "jobj" => dos.writeByte('j')
      case _ => throw new IllegalArgumentException(s"Invalid type $typeStr")
    }
  }

  def writeObject(dos: DataOutputStream, value: Object, objMap: HashMap[String, Object]) {
    if (value == null) {
      writeType(dos, "void")
    } else {
      value.getClass.getName match {
        case "java.lang.String" => {
          writeType(dos, "character")
          writeString(dos, value.asInstanceOf[String])
        }
        case "long" | "java.lang.Long" => {
          writeType(dos, "double")
          writeDouble(dos, value.asInstanceOf[Long].toDouble)
        }
        case "double" | "java.lang.Double" => {
          writeType(dos, "double")
          writeDouble(dos, value.asInstanceOf[Double])
        }
        case "int" | "java.lang.Integer" => {
          writeType(dos, "integer")
          writeInt(dos, value.asInstanceOf[Int])
        }
        case "boolean" | "java.lang.Boolean" => {
          writeType(dos, "logical")
          writeBoolean(dos, value.asInstanceOf[Boolean])
        }
        case "[B" => {
          writeType(dos, "raw")
          writeBytes(dos, value.asInstanceOf[Array[Byte]])
        }
        // TODO: Types not handled right now include
        // byte, char, short, float

        // Handle arrays
        case "[Ljava.lang.String;" => {
          writeType(dos, "list")
          writeStringArr(dos, value.asInstanceOf[Array[String]])
        }
        case "[I" => {
          writeType(dos, "list")
          writeIntArr(dos, value.asInstanceOf[Array[Int]])
        }
        case "[J" => {
          writeType(dos, "list")
          writeDoubleArr(dos, value.asInstanceOf[Array[Long]].map(_.toDouble))
        }
        case "[D" => {
          writeType(dos, "list")
          writeDoubleArr(dos, value.asInstanceOf[Array[Double]])
        }
        case "[Z" => {
          writeType(dos, "list")
          writeBooleanArr(dos, value.asInstanceOf[Array[Boolean]])
        }
        case "[[B" => {
          writeType(dos, "list")
          writeBytesArr(dos, value.asInstanceOf[Array[Array[Byte]]])
        }
        case _ => {
          // Handle array of objects
          if (value.getClass().getName().startsWith("[L")) {
            val objArr = value.asInstanceOf[Array[Object]]
            writeType(dos, "list")
            writeType(dos, "jobj")
            dos.writeInt(objArr.length)
            objArr.foreach(o => writeJObj(dos, o, objMap)) 
          } else {
            writeType(dos, "jobj")
            writeJObj(dos, value, objMap)
          }
        }
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

  def writeJObj(out: DataOutputStream, value: Object, objMap: HashMap[String, Object]) {
    val objId = value.getClass().getName() + "@" + objCounter
    objCounter = objCounter + 1
    objMap.put(objId, value)
    writeString(out, objId)
  }

  def writeIntArr(out: DataOutputStream, value: Array[Int]) {
    writeType(out, "integer")
    out.writeInt(value.length)
    value.foreach(v => out.writeInt(v))
  }

  def writeDoubleArr(out: DataOutputStream, value: Array[Double]) {
    writeType(out, "double")
    out.writeInt(value.length)
    value.foreach(v => out.writeDouble(v))
  }

  def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]) {
    writeType(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeBoolean(out, v))
  }

  def writeStringArr(out: DataOutputStream, value: Array[String]) {
    writeType(out, "character")
    out.writeInt(value.length)
    value.foreach(v => writeString(out, v))
  }

  def writeBytesArr(out: DataOutputStream, value: Array[Array[Byte]]) {
    writeType(out, "raw")
    out.writeInt(value.length)
    value.foreach(v => writeBytes(out, v))
  }
}
