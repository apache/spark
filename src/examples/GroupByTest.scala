import spark.SparkContext
import spark.SparkContext._
import java.util.Random

object GroupByTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: GroupByTest <host> [numSlices] [numKVPairs] [KeySize]")
      System.exit(1)
    }  
    
    var numSlices = if (args.length > 1) args(1).toInt else 2
    var numKVPairs = if (args.length > 2) args(2).toInt else 1000
    var valSize = if (args.length > 3) args(3).toInt else 1000

    val sc = new SparkContext(args(0), "GroupBy Test")
    
//    import java.util.Random
//    
//    var numSlices = 6
//    var numKVPairs = 1000
//    var valSize = 500000
    
    val ranGen = new Random
    
    val pairs1 = sc.parallelize(0 until numSlices, numSlices).flatMap { p =>
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte] (valSize)
        ranGen.nextBytes (byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache
    // Enforce that everything has been calculated and in cache
    pairs1.count
    
//    val pairs2 = sc.parallelize(0 until numSlices, numSlices).flatMap { p =>
//      var arr2 = new Array[(Int, Array[Byte])](numKVPairs)
//      for (i <- 0 until numKVPairs) {
//        val byteArr = new Array[Byte] (valSize)
//        ranGen.nextBytes (byteArr)
//        arr2(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
//      }
//      arr2
//    }.cache
//    // Enforce that everything has been calculated and in cache
//    pairs2.count

    println(pairs1.groupByKey(numSlices).count)
//    pairs2.groupByKey(numSlices).count

//    pairs1.join(pairs2)
  }
}
