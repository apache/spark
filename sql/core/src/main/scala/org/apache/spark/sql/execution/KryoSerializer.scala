package org.apache.spark.sql
package execution

import java.nio.ByteBuffer

import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}
import org.apache.spark.{SparkConf, SparkEnv}

object KryoSerializer {
  @transient lazy val ser: SparkKryoSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SparkKryoSerializer(sparkConf)
  }

  def serialize[T](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}
