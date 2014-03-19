package org.apache.spark.sql
package execution

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Serializer, Kryo}

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.MutablePair

class SparkSqlSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_,_]])
    kryo.register(classOf[Array[Any]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericMutableRow])
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.math.BigDecimal], new BigDecimalSerializer)
    kryo.setReferences(false)
    kryo.setClassLoader(this.getClass.getClassLoader)
    kryo
  }
}

object SparkSqlSerializer {
  @transient lazy val ser: SparkSqlSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SparkSqlSerializer(sparkConf)
  }

  def serialize[T](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}

class BigDecimalSerializer extends Serializer[BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: math.BigDecimal) {
    // TODO: There are probably more efficient representations than strings...
    output.writeString(bd.toString())
  }

  def read(kryo: Kryo, input: Input, tpe: Class[BigDecimal]): BigDecimal = {
    BigDecimal(input.readString())
  }
}
