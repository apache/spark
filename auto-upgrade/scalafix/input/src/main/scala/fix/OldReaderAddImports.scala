/*
 rule=MigrateDeprecatedDataFrameReaderFuns
 */
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SparkSession, Dataset}

object BadReadsAddImports {
  def doMyWork(session: SparkSession, r: RDD[String], dataset: Dataset[String]) = {
    import session.implicits._
    val shouldRewriteBasic = session.read.json(r)
    val r2 = session.sparkContext.parallelize(List("{}"))
    val shouldRewrite = session.read.json(r2)
    val r3: RDD[String] = session.sparkContext.parallelize(List("{}"))
    val shouldRewriteExplicit = session.read.json(r3)
    val noRewrite2 = session.read.json(dataset)
  }
}
