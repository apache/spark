import org.apache.spark._
import org.apache.spark.sql._

object BadHiveContextMagic {
  def hiveContextFunc(sc: SparkContext): SQLContext = {
    val hiveContext1 = SparkSession.builder.enableHiveSupport().getOrCreate().sqlContext
    import hiveContext1.implicits._
    hiveContext1
  }
}
