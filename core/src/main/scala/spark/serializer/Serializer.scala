package spark.serializer

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream

import spark.util.ByteBufferInputStream


/**
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[spark.serializer.SerializerInstance]] objects that do the actual serialization and are
 * guaranteed to only be called from one thread at a time.
 */
trait Serializer {
  def newInstance(): SerializerInstance
}


/**
 * A singleton object that can be used to fetch serializer objects based on the serializer
 * class name. If a previous instance of the serializer object has been created, the get
 * method returns that instead of creating a new one.
 */
object Serializer {

  private val serializers = new ConcurrentHashMap[String, Serializer]
  private var _default: Serializer = _

  def default = _default

  def setDefault(clsName: String): Serializer = {
    _default = get(clsName)
    _default
  }

  def get(clsName: String): Serializer = {
    if (clsName == null) {
      default
    } else {
      var serializer = serializers.get(clsName)
      if (serializer != null) {
        // If the serializer has been created previously, reuse that.
        serializer
      } else this.synchronized {
        // Otherwise, create a new one. But make sure no other thread has attempted
        // to create another new one at the same time.
        serializer = serializers.get(clsName)
        if (serializer == null) {
          val clsLoader = Thread.currentThread.getContextClassLoader
          serializer = Class.forName(clsName, true, clsLoader).newInstance().asInstanceOf[Serializer]
          serializers.put(clsName, serializer)
        }
        serializer
      }
    }
  }
}


/**
 * An instance of a serializer, for use by one thread at a time.
 */
trait SerializerInstance {
  def serialize[T](t: T): ByteBuffer

  def deserialize[T](bytes: ByteBuffer): T

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream

  def serializeMany[T](iterator: Iterator[T]): ByteBuffer = {
    // Default implementation uses serializeStream
    val stream = new FastByteArrayOutputStream()
    serializeStream(stream).writeAll(iterator)
    val buffer = ByteBuffer.allocate(stream.position.toInt)
    buffer.put(stream.array, 0, stream.position.toInt)
    buffer.flip()
    buffer
  }

  def deserializeMany(buffer: ByteBuffer): Iterator[Any] = {
    // Default implementation uses deserializeStream
    buffer.rewind()
    deserializeStream(new ByteBufferInputStream(buffer)).asIterator
  }
}


/**
 * A stream for writing serialized objects.
 */
trait SerializationStream {
  def writeObject[T](t: T): SerializationStream
  def flush(): Unit
  def close(): Unit

  def writeAll[T](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * A stream for reading serialized objects.
 */
trait DeserializationStream {
  def readObject[T](): T
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new spark.util.NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
