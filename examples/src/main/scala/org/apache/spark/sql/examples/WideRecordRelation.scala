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

package org.apache.spark.sql.examples

import org.apache.spark.sql.catalyst.RecordClass
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object WideRecordRelation {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "WideRecordRelation")
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext._

    val rdd = sc.parallelize((1 to 100).map(i => makeRecord(i, s"val_$i")))
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    rdd.registerAsTable("widetable")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sql("SELECT * FROM widetable").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = sql("SELECT COUNT(*) FROM widetable").collect().head.getInt(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = sql("SELECT r1, r2 FROM widetable WHERE r1 < 10")

    println("Result of RDD.map:")
    rddFromSql.map(row => s"r1: ${row(0)}, r2: ${row(1)}").collect.foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    rdd.where('r1 === 1).orderBy('r2.asc).select('r1).collect().foreach(println)

    // Write out an RDD as a parquet file.
    rdd.saveAsParquetFile("pair.parquet")

    // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
    val parquetFile = sqlContext.parquetFile("pair.parquet")

    // Queries can be run using the DSL on parequet files just like the original RDD.
    parquetFile.where('r1 === 1).select('r2 as 'a).collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerAsTable("parquetFile")
    sql("SELECT * FROM parquetFile").collect().foreach(println)
  }
  def makeRecord(i: Int, s: String): WideRecord = {
    new WideRecord(
      i,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s,
      s)
  }
}
class WideRecord (
    r1: Int,
    r2: String,
    r3: String,
    r4: String,
    r5: String,
    r6: String,
    r7: String,
    r8 : String,
    r9: String,
    r10: String,
    r11: String,
    r12: String,
    r13: String,
    r14: String,
    r15: String,
    r16: String,
    r17: String,
    r18: String,
    r19: String,
    r20: String,
    r21: String,
    r22: String,
    r23: String,
    r24: String,
    r25: String)
  extends RecordClass {

  def recordIterator: Iterator[Any] = new scala.collection.Iterator[Any] {
    private var c: Int = 0
    private val cmax = 25
    def hasNext = c < cmax
    def next() = { val result = element(c); c += 1; result }
  }

  def element(n: Int): Any = {
    n match {
      case 0 => this.r1
      case 1 => this.r2
      case 2 => this.r3
      case 3 => this.r4
      case 4 => this.r5
      case 5 => this.r6
      case 6 => this.r7
      case 7 => this.r8
      case 8 => this.r9
      case 9 => this.r10
      case 10 => this.r11
      case 11 => this.r12
      case 12 => this.r13
      case 13 => this.r14
      case 14 => this.r15
      case 15 => this.r16
      case 16 => this.r17
      case 17 => this.r18
      case 18 => this.r19
      case 19 => this.r20
      case 20 => this.r21
      case 21 => this.r22
      case 22 => this.r23
      case 23 => this.r24
      case 24 => this.r25
    }
  }
}
