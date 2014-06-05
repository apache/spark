package org.apache.spark.examples.pythonconverters

import org.apache.spark.api.python.Converter
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import collection.JavaConversions.{mapAsJavaMap, mapAsScalaMap}


class CassandraCQLKeyConverter extends Converter {
  override def convert(obj: Any) = {
    val result = obj.asInstanceOf[java.util.Map[String, ByteBuffer]]
    mapAsJavaMap(result.mapValues(bb => ByteBufferUtil.toInt(bb)))
  }
}

class CassandraCQLValueConverter extends Converter {
  override def convert(obj: Any) = {
    val result = obj.asInstanceOf[java.util.Map[String, ByteBuffer]]
    mapAsJavaMap(result.mapValues(bb => ByteBufferUtil.string(bb)))
  }
}
