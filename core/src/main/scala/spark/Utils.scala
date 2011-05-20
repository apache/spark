package spark

import java.io._
import java.net.InetAddress
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Various utility methods used by Spark.
 */
object Utils {
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close
    return bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char) = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j);
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }

  // Create a temporary directory inside the given parent directory
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File =
  {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory " +
                              "after " + maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }
    return dir
  }

  // Copy all data from an InputStream to an OutputStream
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }

  // Shuffle the elements of a collection into a random order, returning the
  // result in a new collection. Unlike scala.util.Random.shuffle, this method
  // uses a local random number generator, avoiding inter-thread contention.
  def randomize[T](seq: TraversableOnce[T]): Seq[T] = {
    val buf = new ArrayBuffer[T]()
    buf ++= seq
    val rand = new Random()
    for (i <- (buf.size - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = buf(j)
      buf(j) = buf(i)
      buf(i) = tmp
    }
    buf
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4)
   */
  def localIpAddress(): String = {
    // Get local IP as an array of four bytes
    val bytes = InetAddress.getLocalHost().getAddress()
    // Convert the bytes to ints (keeping in mind that they may be negative)
    // and join them into a string
    return bytes.map(b => (b.toInt + 256) % 256).mkString(".")
  }

  /**
   * Get the local machine's hostname
   */
  def localHostName(): String = {
    return InetAddress.getLocalHost().getHostName
  }
}
