package org.apache.spark.examples.pythonconverters

import org.apache.spark.api.python.Converter
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import collection.JavaConversions.{mapAsJavaMap, mapAsScalaMap}


/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts Cassandra
 * output to a Map[String, Int]
 */
class CassandraCQLKeyConverter extends Converter[Any, java.util.Map[String, Int]] {
  override def convert(obj: Any): java.util.Map[String, Int] = {
    val result = obj.asInstanceOf[java.util.Map[String, ByteBuffer]]
    mapAsJavaMap(result.mapValues(bb => ByteBufferUtil.toInt(bb)))
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts Cassandra
 * output to a Map[String, String]
 */
class CassandraCQLValueConverter extends Converter[Any, java.util.Map[String, String]] {
  override def convert(obj: Any): java.util.Map[String, String] = {
    val result = obj.asInstanceOf[java.util.Map[String, ByteBuffer]]
    mapAsJavaMap(result.mapValues(bb => ByteBufferUtil.string(bb)))
  }
}
