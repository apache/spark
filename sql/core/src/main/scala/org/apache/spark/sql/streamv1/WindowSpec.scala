package org.apache.spark.sql.streamv1

trait WindowSpec

class TimeBasedWindow private() extends WindowSpec {
  def over(length: Long): TimeBasedWindow = ???
  def every(interval: Long): TimeBasedWindow = ???
}

object TimeBasedWindow {
  def over(length: Long): TimeBasedWindow = {
    new TimeBasedWindow().over(length)
  }

  def every(interval: Long): TimeBasedWindow = {
    new TimeBasedWindow().every(interval)
  }
}


class GlobalWindow private (interval: Long) extends WindowSpec

object GlobalWindow {
  def every(interval: Long): GlobalWindow = {
    new GlobalWindow(interval)
  }
}

