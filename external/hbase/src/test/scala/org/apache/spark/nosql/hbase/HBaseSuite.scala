package org.apache.spark.nosql.hbase

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkContext, LocalSparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HBaseTestingUtility}
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.spark.sql.catalyst.types.{FloatType, StringType}
import org.apache.spark.sql.{TestData, SchemaRDD}
import org.apache.spark.sql.test.TestSQLContext._

class HBaseSuite
  extends FunSuite
  with LocalSparkContext
  with BeforeAndAfterAll {

  val util = new HBaseTestingUtility()

  override def beforeAll() {
    util.startMiniCluster()
  }

  override def afterAll() {
    util.shutdownMiniCluster()
  }

  test("save SequenceFile as HBase table") {
    sc = new SparkContext("local", "test1")
    val nums = sc.makeRDD(1 to 3).map(x => new Text("a" + x + " 1.0"))

    val table = "test1"
    val rowkeyType = StringType
    val cfBytes = Bytes.toBytes("cf")
    val qualBytes = Bytes.toBytes("qual0")
    val columns = List[HBaseColumn](new HBaseColumn(cfBytes, qualBytes, FloatType))
    val delimiter = ' '

    util.createTable(Bytes.toBytes(table), cfBytes)
    val conf = util.getConfiguration
    val zkHost = conf.get(HConstants.ZOOKEEPER_QUORUM)
    val zkPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)
    val zkNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT)

    HBaseUtils.saveAsHBaseTable(nums, zkHost, zkPort, zkNode, table, rowkeyType, columns, delimiter)

    // Verify results
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
  }

  test("save SchemaRDD as HBase table") {
    val table = "test2"
    val cfBytes = Bytes.toBytes("cf")

    util.createTable(Bytes.toBytes(table), cfBytes)
    val conf = util.getConfiguration
    val zkHost = conf.get(HConstants.ZOOKEEPER_QUORUM)
    val zkPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)
    val zkNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT)

    HBaseUtils.saveAsHBaseTable(TestData.lowerCaseData, zkHost, zkPort, zkNode, table, cfBytes)

    // Verify results
    val htable = new HTable(conf, table)
    val scan = new Scan()
    val rs = htable.getScanner(scan)
    val qualBytes = Bytes.toBytes("l")

    var result = rs.next()
    var i = 1
    while (result != null) {
      val rowkey = Bytes.toInt(result.getRow)
      assert(rowkey == i)
      val value = Bytes.toString(result.getValue(cfBytes, qualBytes))
      assert(('a' + i - 1).toChar + "" == value)
      result = rs.next()
      i += 1
    }

    rs.close()
    htable.close()
  }
}
