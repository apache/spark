package org.apache.spark.graph.util


object HashUtils {

  /**
   * Compute a 64-bit hash value for the given string.
   * See http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
   */
  def hash(str: String): Long = {
    var h = 1125899906842597L
    val len = str.length
    var i = 0

    while (i < len) {
      h = 31 * h + str(i)
      i += 1
    }
    h
  }
}
