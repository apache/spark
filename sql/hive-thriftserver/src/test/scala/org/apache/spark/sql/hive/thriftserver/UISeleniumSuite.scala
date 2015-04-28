package org.apache.spark.sql.hive.thriftserver

import java.sql.{DriverManager, Statement}

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.{SparkConf, SparkContext}
import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.SpanSugar._


import scala.io.Source
import scala.util.Random

/**
 * Created by tianyi on 4/27/15.
 */
class UISeleniumSuite extends FunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _
  var server: HiveThriftServer2 = _
  var hc: HiveContext = _
  val uiPort = 20000 + Random.nextInt(10000)
  val listenPort = 10000 + Random.nextInt(10000)

  override def beforeAll(): Unit = {
    webDriver = new HtmlUnitDriver
    startThriftServer
  }

  override def afterAll(): Unit = {
    if (webDriver != null) {
      webDriver.quit()
    }
    if (server != null) {
      stopThriftServer
    }
  }

  def withMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val user = System.getProperty("user.name")
    Class.forName(classOf[HiveDriver].getCanonicalName)
    val connections = fs.map {
      _ => DriverManager.getConnection(s"jdbc:hive2://localhost:$listenPort/", user, "")
    }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).map { case (s, f) => f(s) }
    } finally {
      statements.map(_.close())
      connections.map(_.close())
    }
  }

  def withJdbcStatement(f: Statement => Unit) {
    withMultipleConnectionJdbcStatement(f)
  }

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
   */
  private def startThriftServer: Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", uiPort.toString)
    hc = new HiveContext(new SparkContext(conf))
    hc.hiveconf.set(ConfVars.HIVE_SERVER2_THRIFT_PORT.toString, listenPort.toString)
    server = new HiveThriftServer2(hc)
    server.init(hc.hiveconf)
    server.start()
    HiveThriftServer2.listener = new HiveThriftServer2Listener(server, hc.conf)
    hc.sparkContext.addSparkListener(HiveThriftServer2.listener)
    HiveThriftServer2.uiTab = Some(new ThriftServerTab(hc.sparkContext))
  }

  private def stopThriftServer: Unit = {
    server.stop()
  }

  test("thrift server ui test") {
    withJdbcStatement(statement =>{
      val queries = Seq(
        "DROP TABLE IF EXISTS test_map",
        "CREATE TABLE test_map(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")

      queries.foreach(statement.execute)

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (hc.sparkContext.ui.get.appUIAddress.stripSuffix("/"))
        find(cssSelector("""ul li a[href*="ThriftServer"]""")) should not be(None)
      }

      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        go to (hc.sparkContext.ui.get.appUIAddress.stripSuffix("/")+"/ThriftServer")
        find(id("sessionstat")) should not be(None)
        find(id("sqlstat")) should not be(None)

        // check whether statements exists
        queries.foreach { line =>
          findAll(cssSelector("""ul table tbody tr td""")).map(_.text).toList should contain (line)
        }
      }
    })
  }
}
