import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by jzhang on 10/22/15.
 */
object SparkSQLExample {

  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val df =sqlContext.read.json("examples/src/main/resources/people.json")
    df.show()

  }
}
