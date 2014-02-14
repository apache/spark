package org.apache.spark
package sql
package shark
package execution


import org.scalatest.{FunSuite, BeforeAndAfterAll}

class ConcurrentHiveSuite extends FunSuite with BeforeAndAfterAll {
  ignore("multiple instances not supported") {
    test("Multiple Hive Instances") {
      (1 to 10).map { i =>
        val ts =
          new TestSharkContext(new SparkContext("local", s"TestSqlContext$i", new SparkConf()))
        ts.executeSql("SHOW TABLES").toRdd.collect()
        ts.executeSql("SELECT * FROM src").toRdd.collect()
        ts.executeSql("SHOW TABLES").toRdd.collect()
      }
    }
  }
}