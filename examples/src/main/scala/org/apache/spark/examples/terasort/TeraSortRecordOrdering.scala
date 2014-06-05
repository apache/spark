package org.apache.spark.examples.terasort


/**
 * Comparator used for terasort. It compares the first ten bytes of the record by
 * first comparing the first 8 bytes, and then the last 2 bytes (using unsafe).
 */
class TeraSortRecordOrdering extends Ordering[Array[Byte]] {

  private[this] val UNSAFE: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get(null).asInstanceOf[sun.misc.Unsafe]
  }

  private[this] val BYTE_ARRAY_BASE_OFFSET: Long = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  override def compare(x: Array[Byte], y: Array[Byte]): Int = {
    val leftWord = UNSAFE.getLong(x, BYTE_ARRAY_BASE_OFFSET)
    val rightWord = UNSAFE.getLong(y, BYTE_ARRAY_BASE_OFFSET)

    val diff = leftWord - rightWord
    if (diff != 0) {
      diff.toInt
    } else {
      UNSAFE.getChar(x, BYTE_ARRAY_BASE_OFFSET + 8) - UNSAFE.getChar(y, BYTE_ARRAY_BASE_OFFSET + 8)
    }
  }
}
