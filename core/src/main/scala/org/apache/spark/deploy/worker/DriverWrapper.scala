package org.apache.spark.deploy.worker

object DriverWrapper {
  def main(args: Array[String]) {
    val c = Console.readChar()
    println(s"Char: $c")
  }
}
