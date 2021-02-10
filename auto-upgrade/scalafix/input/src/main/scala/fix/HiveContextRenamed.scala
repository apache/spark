/*
 rule=MigrateHiveContext
 */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.{HiveContext => HiveCtx}

object BadHiveContextMagic2 {
  def hiveContextFunc(sc: SparkContext): HiveCtx = {
    val hiveContext1 = new HiveCtx(sc)
    import hiveContext1.implicits._
    hiveContext1
  }
}
