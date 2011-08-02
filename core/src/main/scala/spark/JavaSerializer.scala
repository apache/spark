package spark

import java.io._

class JavaSerializationStream(out: OutputStream) extends SerializationStream {
  val objOut = new ObjectOutputStream(out)
  def writeObject[T](t: T) { objOut.writeObject(t) }
  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

class JavaDeserializationStream(in: InputStream) extends DeserializationStream {
  val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
  }

  def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

class JavaSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = outputStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val in = inputStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def outputStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s)
  }

  def inputStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s)
  }
}

class JavaSerializer extends Serializer {
  def newInstance(): SerializerInstance = new JavaSerializerInstance
}
