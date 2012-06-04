package spark.repl

import scala.collection.mutable.Set

object Main {
  private var _interp: SparkILoop = null
  
  def interp = _interp
  
  def interp_=(i: SparkILoop) { _interp = i }
  
  def main(args: Array[String]) {
    _interp = new SparkILoop
    _interp.process(args)
  }
}
