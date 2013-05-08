package spark.examples

import spark._
import spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HBaseTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "HBaseTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    val conf = HBaseConfiguration.create()

    // Other options for configuring scan behavior are available. More information available at 
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, args(1))

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(args(1))) {
      val tableDesc = new HTableDescriptor(args(1))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    System.exit(0)
  }
}
