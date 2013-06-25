package spark

import org.scalatest.{ BeforeAndAfter, FunSuite }
import spark.SparkContext._
import spark.rdd.JdbcRDD
import java.sql._

class JdbcRDDSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  before {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;create=true")
    try {
      val create = conn.createStatement
      create.execute("""
        CREATE TABLE FOO(
          ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
          DATA INTEGER
        )""")
      create.close
      val insert = conn.prepareStatement("INSERT INTO FOO(DATA) VALUES(?)")
      (1 to 100).foreach { i =>
        insert.setInt(1, i * 2)
        insert.executeUpdate
      }
      insert.close
    } catch {
      case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
    } finally {
      conn.close
    }
  }

  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val rdd = new JdbcRDD(
      sc,
      () => { DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb") },
      "SELECT DATA FROM FOO WHERE ? <= ID AND ID <= ?",
      1, 100, 3,
      (r: ResultSet) => { r.getInt(1) } ).cache

    assert(rdd.count === 100)
    assert(rdd.reduce(_+_) === 10100)
  }

  after {
    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true")
    } catch {
      case se: SQLException if se.getSQLState == "XJ015" =>
        // normal shutdown
    }
  }
}
