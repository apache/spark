package spark.streaming

import scala.collection.mutable.Map

object IdealPerformance {
  val base: String = "The medium researcher counts around the pinched troop The empire breaks " +
  		"Matei Matei announces HY with a theorem " 

  def main (args: Array[String]) {
    val sentences: String = base * 100000
    
    for (i <- 1 to 30) {
      val start = System.nanoTime
      
      val words = sentences.split(" ")
      
      val pairs = words.map(word => (word, 1))
      
      val counts = Map[String, Int]()
      
      println("Job " + i + " position A at " + (System.nanoTime - start) / 1e9)
      
      pairs.foreach((pair) => {
        var t = counts.getOrElse(pair._1, 0)
        counts(pair._1) = t + pair._2
      })
      println("Job " + i + " position B at " + (System.nanoTime - start) / 1e9)
      
      for ((word, count) <- counts) {
        print(word + " " + count + "; ")
      }
      println
      println("Job " + i + " finished in " + (System.nanoTime - start) / 1e9)
    }
  }
}