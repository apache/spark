package spark.ml

import spark.{RDD, SparkContext}

object MLUtils {

  // Helper methods to load and save data
  // Data format:
  // <l>, <f1> <f2> ...
  // where <f1>, <f2> are feature values in Double and 
  //       <l> is the corresponding label as Double
  def loadData(sc: SparkContext, dir: String) = {
    val data = sc.textFile(dir).map{ line => 
      val parts = line.split(",")
      val label = parts(0).toDouble
      val features = parts(1).trim().split(" ").map(_.toDouble)
      (label, features)
    }
    data
  }

  def saveData(data: RDD[(Double, Array[Double])], dir: String) {
    val dataStr = data.map(x => x._1 + "," + x._2.mkString(" "))
    dataStr.saveAsTextFile(dir)
  }

}
