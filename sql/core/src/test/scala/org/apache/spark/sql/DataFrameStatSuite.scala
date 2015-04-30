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

package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class DataFrameStatSuite extends FunSuite  {

  val sqlCtx = TestSQLContext

  test("Frequent Items") {
    def toLetter(i: Int): String = (i + 96).toChar.toString
    val rows = Array.tabulate(1000)(i => if (i % 3 == 0) (1, toLetter(1)) else (i, toLetter(i)))
    val rowRdd = sqlCtx.sparkContext.parallelize(rows.map(v => Row(v._1, v._2)))
    val schema = StructType(StructField("numbers", IntegerType, false) ::
                            StructField("letters", StringType, false) :: Nil)
    val df = sqlCtx.createDataFrame(rowRdd, schema)

    val results = df.stat.freqItems(Array("numbers", "letters"), 0.1)
    val items = results.collect().head
    assert(items.getSeq(0).contains(1),
      "1 should be the frequent item for column 'numbers")
    assert(items.getSeq(1).contains(toLetter(1)),
      s"${toLetter(1)} should be the frequent item for column 'letters'")
  }
}
