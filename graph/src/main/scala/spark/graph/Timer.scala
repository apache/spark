package spark.graph


class Timer {

  var lastTime = System.currentTimeMillis

  def tic = {
    val currentTime = System.currentTimeMillis
    val elapsedTime = (currentTime - lastTime)/1000.0
    lastTime = currentTime
    elapsedTime
  }
}
