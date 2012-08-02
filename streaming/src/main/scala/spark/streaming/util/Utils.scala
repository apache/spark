package spark.streaming.util

object Utils {
  def time(func: => Unit): Long = {
    val t = System.currentTimeMillis
    func
    (System.currentTimeMillis - t)
  }
}