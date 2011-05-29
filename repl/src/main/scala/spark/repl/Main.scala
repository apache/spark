package spark.repl

import scala.collection.mutable.Set

object Main {
  private var _interp: SparkInterpreterLoop = null
  
  def interp = _interp
  
  private[repl] def interp_=(i: SparkInterpreterLoop) { _interp = i }
  
  def main(args: Array[String]) {
    _interp = new SparkInterpreterLoop
    _interp.main(args)
  }
}
