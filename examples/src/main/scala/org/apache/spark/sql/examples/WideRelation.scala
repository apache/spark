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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object WideRelation {
  def main(args: Array[String]) {
    println("org.apache.spark.sql.examples.WideRelation master")
    val sc = new SparkContext("local", "WideRelation")
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
    r25: String) extends Product with Serializable {
  
  override def productArity : scala.Int = {
    25
  }
  override def canEqual(that : scala.Any) : scala.Boolean = that.isInstanceOf[WideRecord]

  override def productElement(n : scala.Int) : scala.Any = {
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

