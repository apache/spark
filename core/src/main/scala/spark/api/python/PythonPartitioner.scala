package spark.api.python

import spark.Partitioner

import java.util.Arrays

/**
 * A [[spark.Partitioner]] that performs handling of byte arrays, for use by the Python API.
 *
 * Stores the unique id() of the Python-side partitioning function so that it is incorporated into
 * equality comparisons.  Correctness requires that the id is a unique identifier for the
 * lifetime of the program (i.e. that it is not re-used as the id of a different partitioning
 * function).  This can be ensured by using the Python id() function and maintaining a reference
 * to the Python partitioning function so that its id() is not reused.
 */
private[spark] class PythonPartitioner(
  override val numPartitions: Int,
  val pyPartitionFunctionId: Long)
  extends Partitioner {

  override def getPartition(key: Any): Int = {
    if (key == null) {
      return 0
    }
    else {
      val hashCode = {
        if (key.isInstanceOf[Array[Byte]]) {
          Arrays.hashCode(key.asInstanceOf[Array[Byte]])
        } else {
          key.hashCode()
        }
      }
      val mod = hashCode % numPartitions
      if (mod < 0) {
        mod + numPartitions
      } else {
        mod // Guard against negative hash codes
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: PythonPartitioner =>
      h.numPartitions == numPartitions && h.pyPartitionFunctionId == pyPartitionFunctionId
    case _ =>
      false
  }
}
