/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.sql._

import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}
import org.apache.spark.util.Utils
import org.apache.derby.iapi.error.StandardException

class JdbcRDDSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {
  
  before {
    Utils.classForName("org.apache.derby.jdbc.EmbeddedDriver")
  }
  
  trait WithEnvironment {    
    sc = new SparkContext("local", "test")
    def executeDBOperation(op: (Connection) => Unit): Unit = {
      val conn = getConnection()
      try {
        op(conn)
      } finally {
        conn.close()
      }
    }
    
    def getConnection(): Connection = {
      DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;create=true")
    }
    
    def dropTable(name: String): Unit = {
      executeDBOperation { conn =>
       try {
          val create = conn.createStatement
          create.execute(s"DROP TABLE ${name}")
          create.close()
        } catch {
          case e: SQLSyntaxErrorException if e.getSQLState == "42Y55" =>
          // table not exists
        }
      }
    }
  }
  
  trait WithTable extends WithEnvironment {
    val Table = "test"
    
    def with100SmallNumericIdRecords(mapRecord: (Int) => (Int, Int)): Unit = {
      dropTable(Table)
      executeDBOperation { conn => 
        try {
          val create = conn.createStatement
          create.execute(s"""
            CREATE TABLE ${Table}(
              ID INTEGER NOT NULL,
              DATA INTEGER
            )""")
          create.close()
          
          val insert = conn.prepareStatement(s"INSERT INTO ${Table} VALUES(?,?)")
          (1 to 100).foreach { i =>
            val (id, data) = mapRecord(i)
            insert.setInt(1, id)
            insert.setInt(2, data)
            insert.executeUpdate
          }
          insert.close()
        } catch {
          case e: SQLException if e.getSQLState == "X0Y32" =>
          // table exists
        }
      }
    }
    
    def with100LargeNumericIdRecords(mapRecord: (Int) => (Long, Int)): Unit = {
      dropTable(Table)
      executeDBOperation { conn => 
        try {
          val create = conn.createStatement
          create.execute(s"CREATE TABLE ${Table}(ID BIGINT NOT NULL, DATA INTEGER)")
          create.close()
          val insert = conn.prepareStatement(s"INSERT INTO ${Table} VALUES(?,?)")
          (1 to 100).foreach { i =>
            val (id, data) = mapRecord(i)
            insert.setLong(1, id)
            insert.setInt(2, data)
            insert.executeUpdate
          }
          insert.close()
        } catch {
          case e: SQLException if e.getSQLState == "X0Y32" =>
          // table exists
        }
      }
    }

    def withStringIdRecords(numRecords: Int)(mapRecord: (Int) => (String, Int)): Unit = {
      dropTable(Table)
      executeDBOperation { conn => 
        try {
          val create = conn.createStatement
          create.execute(s"CREATE TABLE ${Table}(ID VARCHAR(255) NOT NULL, DATA INTEGER)")
          create.close()
          val insert = conn.prepareStatement(s"INSERT INTO ${Table} VALUES(?,?)")
          (1 to numRecords).foreach { i =>
            val (id, data) = mapRecord(i)
            insert.setString(1, id)
            insert.setInt(2, data)
            insert.executeUpdate
          }
          insert.close()
        } catch {
          case e: SQLException if e.getSQLState == "X0Y32" =>
          // table exists
        }
      }
    }
  }

  test("100 small numeric ID records") { new WithTable {
    with100SmallNumericIdRecords { recordNumber =>
      (recordNumber, recordNumber * 2)
    }
    
    val rdd = new JdbcRDD(
      sc,
      () => DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb"),
      s"SELECT DATA FROM ${Table} WHERE ID >= ? AND ID <= ?",
      1, 100, 3,
      (r: ResultSet) => { r.getInt(1) } ).cache()

    assert(rdd.count === 100)
    assert(rdd.reduce(_ + _) === 10100)
  }}

  test("100 large numeric ID records overflow") { new WithTable {
    with100LargeNumericIdRecords { recordNumber =>
      (100000000000000000L +  4000000000000000L * recordNumber, recordNumber)
    }
    
    val rdd = new JdbcRDD(
      sc,
      () => DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb"),
      s"SELECT DATA FROM ${Table} WHERE ID >= ? AND ID <= ?",
      1131544775L, 567279358897692673L, 20,
      (r: ResultSet) => { r.getInt(1) } ).cache()
      
    assert(rdd.count === 100)
    assert(rdd.reduce(_ + _) === 5050)
  }}
  
  test("some string ID records") { new WithTable {
    val ids = Seq("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    withStringIdRecords(ids.size) { recordNumber =>
      (ids(recordNumber - 1), recordNumber)
    }
    
    val rdd = new JdbcRDD(
      sc,
      () => DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb"),
      s"SELECT DATA FROM ${Table} WHERE ? <= ID AND ID <= ?",
      JdbcPartitions(JdbcPartition.stringPartition("a", "c"), JdbcPartition.stringPartition("d", "j")),
      (r: ResultSet) => { r.getInt(1) } ).cache()
      
    assert(rdd.count === ids.size)
    assert(rdd.reduce(_ + _) === 55)
  }}
  
  case class Order(id: Long, itemPrices: Seq[Double])
  
  trait WithOrderTables extends WithEnvironment {
    val OrderTable = "customer_order"
    val OrderItemTable = "order_item"
    
    def withOrders(orders: Order*): Unit = {
      createTables()
      insertOrders(orders)
    }
    
    private def createTables() {
      dropTable(OrderTable)
      dropTable(OrderItemTable)
      executeDBOperation { conn => 
        try {
          val create = conn.createStatement
          create.execute(s"""
            CREATE TABLE ${OrderTable}(
              ID BIGINT NOT NULL)
          """)
           
           create.execute(s"""
            CREATE TABLE ${OrderItemTable}(
              ID BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
              orderID BIGINT NOT NULL,
              price DECIMAL(20,2) NOT NULL)
          """)
          create.close()
        } catch {
          case e: SQLException if e.getSQLState == "X0Y32" => throw e
          // table exists
        }
      }
    }
    
    private def insertOrders(orders: Seq[Order]): Unit = {
      executeDBOperation { conn => 
        try {
          val orderInsert = conn.prepareStatement(s"INSERT INTO ${OrderTable} VALUES(?)")
          val orderItemInsert = conn.prepareStatement(s"INSERT INTO ${OrderItemTable}(orderID, price) VALUES(?,?)")
          def insertOrder(order: Order): Unit = {
            orderInsert.setLong(1, order.id)
            orderInsert.executeUpdate
            
            order.itemPrices.foreach { price => 
              orderItemInsert.setLong(1, order.id)
              orderItemInsert.setDouble(2, price)
              orderItemInsert.executeUpdate()
            }
          }
          
          orders.foreach(insertOrder)
          
          orderItemInsert.close()
          orderInsert.close()
        } catch {
          case e: SQLException if e.getSQLState == "X0Y32" => throw e
          // table exists
        }
      }
    }
  }
  
  test("multiple lower&upper bounds in subqueries") { new WithOrderTables {
    withOrders(
        Order(id = 1, itemPrices = Seq(10.2, 15.1)),
        Order(id = 2, itemPrices = Seq(14.9, 11.5)),
        Order(id = 3, itemPrices = Seq(8.2, 16.5)),
        Order(id = 4, itemPrices = Seq(18.5, 19.5)),
        //two more unused records below.
        Order(id = 5, itemPrices = Seq(1.5, 100.5)),
        Order(id = 6, itemPrices = Seq(2.5, 100.5)))
        
    val sql = s"""
      SELECT o.ID, mp.maxPrice, mp.maxPrice>15
        FROM ${OrderTable} as o 
          LEFT JOIN (SELECT orderID, max(price) as maxPrice
            FROM ${OrderItemTable} 
            WHERE orderID >= ? and orderID <= ?
            GROUP BY orderID) as mp on mp.orderID = o.ID
        WHERE ? <= o.ID AND o.ID <= ?
    """
    val rdd = new JdbcRDD(
      sc,
      () => DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb"),
      sql,
      JdbcPartitions(lowerBound = 1, upperBound = 4, numPartitions = 2, 
        lowerBoundParameterIndexes = Seq(1, 3), upperBoundParameterIndexes = Seq(2, 4)),
      (r: ResultSet) => { (r.getDouble(2), r.getBoolean(3)) } ).cache()
    
    assert(rdd.count === 4)
    
    val (totalMaxPrice, totalGreaterThan15) = rdd.aggregate((0.0, 0))(
        (acc, value) => {
          def trueAsOne(greater: Boolean):Int = if (greater) 1 else 0
          
          (acc._1 + value._1, acc._2 + trueAsOne(value._2))
        },
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    assert(totalMaxPrice === 66.0)
    assert(totalGreaterThan15 === 3)
  }}
    

  after {
    try {
      DriverManager.getConnection("jdbc:derby:target/JdbcRDDSuiteDb;shutdown=true")
    } catch {
      case se: SQLException if se.getSQLState == "08006" =>
        // Normal single database shutdown
        // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
    }
  }
}
