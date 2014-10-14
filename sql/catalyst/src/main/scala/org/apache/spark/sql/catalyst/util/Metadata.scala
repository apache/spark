package org.apache.spark.sql.catalyst.util

import scala.collection.mutable

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.reflect.ClassTag

sealed class Metadata private[util] (val map: Map[String, Any]) extends Serializable {

  def getInt(key: String): Int = get(key)

  def getDouble(key: String): Double = get(key)

  def getBoolean(key: String): Boolean = get(key)

  def getString(key: String): String = get(key)

  def getMetadata(key: String): Metadata = get(key)

  def getIntArray(key: String): Array[Int] = getArray(key)

  def getDoubleArray(key: String): Array[Double] = getArray(key)

  def getBooleanArray(key: String): Array[Boolean] = getArray(key)

  def getStringArray(key: String): Array[String] = getArray(key)

  def getMetadataArray(key: String): Array[Metadata] = getArray(key)

  def toJson: String = {
    compact(render(Metadata.toJValue(this)))
  }

  private def get[T](key: String): T = {
    map(key).asInstanceOf[T]
  }

  private def getArray[T: ClassTag](key: String): Array[T] = {
    map(key).asInstanceOf[Seq[T]].toArray
  }

  override def toString: String = toJson
}

object Metadata {

  def empty: Metadata = new Metadata(Map.empty)

  def fromJson(json: String): Metadata = {
    val map = parse(json).values.asInstanceOf[Map[String, Any]]
    fromMap(map.toMap)
  }

  private def fromMap(map: Map[String, Any]): Metadata = {
    val builder = new MetadataBuilder
    map.foreach {
      case (key, value: Int) =>
        builder.putInt(key, value)
      case (key, value: BigInt) =>
        builder.putInt(key, value.toInt)
      case (key, value: Double) =>
        builder.putDouble(key, value)
      case (key, value: Boolean) =>
        builder.putBoolean(key, value)
      case (key, value: String) =>
        builder.putString(key, value)
      case (key, value: Map[_, _]) =>
        builder.putMetadata(key, fromMap(value.asInstanceOf[Map[String, Any]]))
      case (key, value: Seq[_]) =>
        if (value.isEmpty) {
          builder.putIntArray(key, Seq.empty)
        } else {
          value.head match {
            case _: Int =>
              builder.putIntArray(key, value.asInstanceOf[Seq[Int]].toSeq)
            case _: BigInt =>
              builder.putIntArray(key, value.asInstanceOf[Seq[BigInt]].map(_.toInt).toSeq)
            case _: Double =>
              builder.putDoubleArray(key, value.asInstanceOf[Seq[Double]].toSeq)
            case _: Boolean =>
              builder.putBooleanArray(key, value.asInstanceOf[Seq[Boolean]].toSeq)
            case _: String =>
              builder.putStringArray(key, value.asInstanceOf[Seq[String]].toSeq)
            case _: Map[String, Any] =>
              builder.putMetadataArray(
                key, value.asInstanceOf[Seq[Map[String, Any]]].map(fromMap).toSeq)
            case other =>
              throw new RuntimeException(s"Do not support array of type ${other.getClass}.")
          }
        }
      case other =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
    builder.build()
  }

  private def toJValue(obj: Any): JValue = {
    obj match {
      case map: Map[_, _] =>
        val fields = map.toList.map { case (k: String, v) => (k, toJValue(v)) }
        JObject(fields)
      case arr: Seq[_] =>
        val values = arr.toList.map(toJValue)
        JArray(values)
      case x: Int =>
        JInt(x)
      case x: Double =>
        JDouble(x)
      case x: Boolean =>
        JBool(x)
      case x: String =>
        JString(x)
      case x: Metadata =>
        toJValue(x.map)
      case other =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
  }
}

class MetadataBuilder {

  private val map: mutable.Map[String, Any] = mutable.Map.empty

  def withMetadata(metadata: Metadata): this.type = {
    map ++= metadata.map
    this
  }

  def putInt(key: String, value: Int): this.type = put(key, value)

  def putDouble(key: String, value: Double): this.type = put(key, value)

  def putBoolean(key: String, value: Boolean): this.type = put(key, value)

  def putString(key: String, value: String): this.type = put(key, value)

  def putMetadata(key: String, value: Metadata): this.type = put(key, value)

  def putIntArray(key: String, value: Seq[Int]): this.type = put(key, value)

  def putDoubleArray(key: String, value: Seq[Double]): this.type = put(key, value)

  def putBooleanArray(key: String, value: Seq[Boolean]): this.type = put(key, value)

  def putStringArray(key: String, value: Seq[String]): this.type = put(key, value)

  def putMetadataArray(key: String, value: Seq[Metadata]): this.type = put(key, value)

  def build(): Metadata = {
    new Metadata(map.toMap)
  }

  private def put(key: String, value: Any): this.type = {
    map.put(key, value)
    this
  }
}
