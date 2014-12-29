package edu.berkeley.cs.amplab.sparkr

import java.io.DataInputStream
import java.io.DataOutputStream

object SerializeJavaR {

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

  def readIntArr(in: DataInputStream) = {
    val len = readInt(in)
    val typeStr = readString(in)
    assert(typeStr == "integer")
    (0 until len).map(_ => readInt(in)).toArray
  }

  def readDoubleArr(in: DataInputStream) = {
    val len = readInt(in)
    val typeStr = readString(in)
    assert(typeStr == "double")
    (0 until len).map(_ => readDouble(in)).toArray
  }

  def readBooleanArr(in: DataInputStream) = {
    val len = readInt(in)
    val typeStr = readString(in)
    assert(typeStr == "logical")
    (0 until len).map(_ => readBoolean(in)).toArray
  }

  def readStringArr(in: DataInputStream) = {
    val len = readInt(in)
    val typeStr = readString(in)
    assert(typeStr == "character")
    (0 until len).map(_ => readString(in)).toArray
  }

  def readStringMap(in: DataInputStream) = {
    val len = readInt(in)
    if (len > 0) {
      val typeStr = readString(in)
      assert(typeStr == "character")
    }
    val keys = (0 until len).map(_ => readString(in))
    val values = (0 until len).map(_ => readString(in))
    keys.zip(values).toMap
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
  }

  def writeIntArr(out: DataOutputStream, value: Array[Int]) {
    out.writeInt(value.length)
    writeString(out, "integer")
    value.foreach(v => out.writeInt(v))
  }

  def writeDoubleArr(out: DataOutputStream, value: Array[Double]) {
    out.writeInt(value.length)
    writeString(out, "double")
    value.foreach(v => out.writeDouble(v))
  }

  def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]) {
    out.writeInt(value.length)
    writeString(out, "logical")
    value.foreach(v => writeBoolean(out, v))
  }

  def writeStringArr(out: DataOutputStream, value: Array[String]) {
    out.writeInt(value.length)
    writeString(out, "character")
    value.foreach(v => writeString(out, v))
  }

  def writeStringMap(out: DataOutputStream, value: Map[String, String]) {
    out.writeInt(value.size)
    if (value.size > 0) {
      writeString(out, "character")
      // TODO: Make writeStringArr work on Iterable ?
      writeStringArr(out, value.keys.toArray) 
      writeStringArr(out, value.values.toArray)
    }
  }
}
