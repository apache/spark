package spark.examples

import spark._
import spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HBaseTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "HBaseTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, args(1))

    // Initialize hBase tables if necessary
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(args(1))) {
      val colDesc = new HColumnDescriptor(args(2))
      val tableDesc = new HTableDescriptor(args(1))
      tableDesc.addFamily(colDesc)
      admin.createTable(tableDesc)
    }

    val hBaseRDD = new NewHadoopRDD(sc, classOf[TableInputFormat], 
                        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                        classOf[org.apache.hadoop.hbase.client.Result], conf)

    hBaseRDD.count()

    System.exit(0)
  }
}
