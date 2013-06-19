package spark.ui

import spark.SparkContext
import spark.scheduler.SparkListener

private[spark]
class JobProgressUI(sc: SparkContext) {
  sc.addSparkListener()
}

class JobProgressListener extends SparkListener {

}

