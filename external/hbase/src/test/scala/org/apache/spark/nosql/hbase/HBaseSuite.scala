package org.apache.spark.nosql.hbase

import org.scalatest.FunSuite
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkContext, LocalSparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HBaseTestingUtility}
import org.apache.hadoop.hbase.client.{Scan, HTable}

class HBaseSuite extends FunSuite with LocalSparkContext {

  test("write SequenceFile using HBase") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(1 to 3).map(x => new Text("a" + x + " 1.0"))

    val table = "test"
    val rowkeyType = HBaseType.String
    val cfBytes = Bytes.toBytes("cf")
    val qualBytes = Bytes.toBytes("qual0")
    val columns = List[HBaseColumn](new HBaseColumn(cfBytes, qualBytes, HBaseType.Float))
    val delimiter = ' '

    val util = new HBaseTestingUtility()
    util.startMiniCluster()
    util.createTable(Bytes.toBytes(table), cfBytes)
    val conf = util.getConfiguration
    val zkHost = conf.get(HConstants.ZOOKEEPER_QUORUM)
    val zkPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)
    val zkNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT)

    HBaseUtils.saveAsHBaseTable(nums, zkHost, zkPort, zkNode, table, rowkeyType, columns, delimiter)

    val htable = new HTable(conf, table)
    val scan = new Scan()
    val rs = htable.getScanner(scan)

    var result = rs.next()
    var i = 1
    while (result != null) {
      val rowkey = Bytes.toString(result.getRow)
      assert(rowkey == "a" + i)
      val value = Bytes.toFloat(result.getValue(cfBytes, qualBytes))
      assert(value == 1.0)
      result = rs.next()
      i += 1
    }

    rs.close()
    htable.close()
    util.shutdownMiniCluster()
  }
}
