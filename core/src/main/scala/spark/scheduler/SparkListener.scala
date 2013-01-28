package spark.scheduler

trait SparkListener {
  def onStageCompleted(stageCompleted: StageCompleted)
}

sealed trait SparkListenerEvents

case class StageCompleted(val stageInfo: StageInfo) extends SparkListenerEvents
