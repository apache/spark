package spark

import java.io.File
import java.io.{FileOutputStream,FileInputStream}
import java.util.LinkedHashMap
import java.util.UUID

// TODO: error handling
// TODO: cache into a separate directory using Utils.createTempDir
// TODO: after reading an entry from disk, put it into the cache

class DiskSpillingCache extends BoundedMemoryCache {
  private val diskMap = new LinkedHashMap[Any, File](32, 0.75f, true)

  override def get(key: Any): Any = {
    synchronized {
      val ser = Serializer.newInstance()
      super.get(key) match {
        case bytes: Any => // found in memory
          ser.deserialize(bytes.asInstanceOf[Array[Byte]])

        case _ => diskMap.get(key) match {
          case file: Any => // found on disk
            val startTime = System.currentTimeMillis
            val bytes = new Array[Byte](file.length.toInt)
            new FileInputStream(file).read(bytes)
            val timeTaken = System.currentTimeMillis - startTime
            logInfo("Reading key %s of size %d bytes from disk took %d ms".format(
              key, file.length, timeTaken))
            super.put(key, bytes)
            ser.deserialize(bytes.asInstanceOf[Array[Byte]])

          case _ => // not found
            null
        }
      }
    }
  }

  override def put(key: Any, value: Any) {
    var ser = Serializer.newInstance()
    super.put(key, ser.serialize(value))
  }

  /**
   * Spill the given entry to disk. Assumes that a lock is held on the
   * DiskSpillingCache.  Assumes that entry.value is a byte array.
   */
  override protected def dropEntry(key: Any, entry: Entry) {
    logInfo("Spilling key %s of size %d to make space".format(
      key, entry.size))
    val cacheDir = System.getProperty(
      "spark.DiskSpillingCache.cacheDir",
      System.getProperty("java.io.tmpdir"))
    val file = new File(cacheDir, "spark-dsc-" + UUID.randomUUID.toString)
    val stream = new FileOutputStream(file)
    stream.write(entry.value.asInstanceOf[Array[Byte]])
    stream.close()
    diskMap.put(key, file)
  }
}
