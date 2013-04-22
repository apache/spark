package spark.streaming

case class Duration (private val millis: Long) {

  def < (that: Duration): Boolean = (this.millis < that.millis)

  def <= (that: Duration): Boolean = (this.millis <= that.millis)

  def > (that: Duration): Boolean = (this.millis > that.millis)

  def >= (that: Duration): Boolean = (this.millis >= that.millis)

  def + (that: Duration): Duration = new Duration(millis + that.millis)

  def - (that: Duration): Duration = new Duration(millis - that.millis)

  def * (times: Int): Duration = new Duration(millis * times)

  def / (that: Duration): Double = millis.toDouble / that.millis.toDouble

  def isMultipleOf(that: Duration): Boolean =
    (this.millis % that.millis == 0)

  def min(that: Duration): Duration = if (this < that) this else that

  def max(that: Duration): Duration = if (this > that) this else that

  def isZero: Boolean = (this.millis == 0)

  override def toString: String = (millis.toString + " ms")

  def toFormattedString: String = millis.toString

  def milliseconds: Long = millis
}


/**
 * Helper object that creates instance of [[spark.streaming.Duration]] representing
 * a given number of milliseconds.
 */
object Milliseconds {
  def apply(milliseconds: Long) = new Duration(milliseconds)
}

/**
 * Helper object that creates instance of [[spark.streaming.Duration]] representing
 * a given number of seconds.
 */
object Seconds {
  def apply(seconds: Long) = new Duration(seconds * 1000)
}

/**
 * Helper object that creates instance of [[spark.streaming.Duration]] representing
 * a given number of minutes.
 */
object Minutes {
  def apply(minutes: Long) = new Duration(minutes * 60000)
}


