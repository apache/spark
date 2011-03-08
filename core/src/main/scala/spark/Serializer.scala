package spark

import java.io.{InputStream, OutputStream}

trait SerializationStream {
  def writeObject[T](t: T): Unit
  def flush(): Unit
  def close(): Unit
}

trait DeserializationStream {
  def readObject[T](): T
  def close(): Unit
}

trait Serializer {
  def serialize[T](t: T): Array[Byte]
  def deserialize[T](bytes: Array[Byte]): T
  def outputStream(s: OutputStream): SerializationStream
  def inputStream(s: InputStream): DeserializationStream
}

trait SerializationStrategy {
  def newSerializer(): Serializer
}

object Serializer {
  var strat: SerializationStrategy = null

  def initialize() {
    val cls = System.getProperty("spark.serialization",
      "spark.JavaSerialization")
    strat = Class.forName(cls).newInstance().asInstanceOf[SerializationStrategy]
  }

  // Return a serializer ** for use by a single thread **
  def newInstance(): Serializer = {
    strat.newSerializer()
  }
}
