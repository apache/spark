package spark

import java.io.File
import java.io.{FileOutputStream,FileInputStream}
import java.io.IOException
import java.util.LinkedHashMap
import java.util.UUID

// TODO: cache into a separate directory using Utils.createTempDir
// TODO: clean up disk cache afterwards
class DiskSpillingCache extends BoundedMemoryCache {
  private val diskMap = new LinkedHashMap[(Any, Int), File](32, 0.75f, true)

  override def get(datasetId: Any, partition: Int): Any = {
    synchronized {
      val ser = SparkEnv.get.serializer.newInstance()
      super.get(datasetId, partition) match {
        case bytes: Any => // found in memory
          ser.deserialize(bytes.asInstanceOf[Array[Byte]])

        case _ => diskMap.get((datasetId, partition)) match {
          case file: Any => // found on disk
            try {
              val startTime = System.currentTimeMillis
              val bytes = new Array[Byte](file.length.toInt)
              new FileInputStream(file).read(bytes)
              val timeTaken = System.currentTimeMillis - startTime
              logInfo("Reading key (%s, %d) of size %d bytes from disk took %d ms".format(
                datasetId, partition, file.length, timeTaken))
              super.put(datasetId, partition, bytes)
              ser.deserialize(bytes.asInstanceOf[Array[Byte]])
            } catch {
              case e: IOException =>
                logWarning("Failed to read key (%s, %d) from disk at %s: %s".format(
                  datasetId, partition, file.getPath(), e.getMessage()))
                diskMap.remove((datasetId, partition)) // remove dead entry
                null
            }

          case _ => // not found
            null
        }
      }
    }
  }

  override def put(datasetId: Any, partition: Int, value: Any): Long = {
    var ser = SparkEnv.get.serializer.newInstance()
    super.put(datasetId, partition, ser.serialize(value))
  }

  /**
   * Spill the given entry to disk. Assumes that a lock is held on the
   * DiskSpillingCache.  Assumes that entry.value is a byte array.
   */
  override protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
    logInfo("Spilling key (%s, %d) of size %d to make space".format(
      datasetId, partition, entry.size))
    val cacheDir = System.getProperty(
      "spark.diskSpillingCache.cacheDir",
      System.getProperty("java.io.tmpdir"))
    val file = new File(cacheDir, "spark-dsc-" + UUID.randomUUID.toString)
    try {
      val stream = new FileOutputStream(file)
      stream.write(entry.value.asInstanceOf[Array[Byte]])
      stream.close()
      diskMap.put((datasetId, partition), file)
    } catch {
      case e: IOException =>
        logWarning("Failed to spill key (%s, %d) to disk at %s: %s".format(
          datasetId, partition, file.getPath(), e.getMessage()))
        // Do nothing and let the entry be discarded
    }
  }
}
