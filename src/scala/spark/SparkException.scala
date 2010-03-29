package spark

class SparkException(message: String) extends Exception(message) {
  def this(message: String, errorCode: Int) {
    this("%s (error code: %d)".format(message, errorCode))
  }
}
