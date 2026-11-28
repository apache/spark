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

package org.apache.spark.sql.execution.datasources

import java.time.LocalDate
import java.util.{HashMap, Map => JMap, TreeMap}

import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.io.{DataInputBuffer, DataOutputBuffer}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.RebaseDateTime.rebaseGregorianToJulianDays

class DaysWritableSuite extends SparkFunSuite {
  private val days = Seq("1500-01-01", "1969-12-31", "2003-01-13", "2011-07-19")
    .map(LocalDate.parse(_).toEpochDay.toInt)

  private def checkComparison(value: DaysWritable, day: Int): Unit = {
    val plain = new DateWritable(rebaseGregorianToJulianDays(day))
    assert(value.gregorianDays == day)
    assert(value.getDays == plain.getDays)
    assert(value.compareTo(plain) == 0)
    assert(plain.compareTo(value) == 0)
    assert(value == plain)
    assert(plain == value)
    assert(value.hashCode() == plain.hashCode())
    assert(new DateWritable(value).getDays == plain.getDays)
  }

  test("SPARK-61763: constructed dates compare consistently with DateWritable") {
    days.foreach { day =>
      val value = new DaysWritable(day)
      checkComparison(value, day)
      checkComparison(new DaysWritable(value), day)
      checkComparison(new DaysWritable(new DateWritable(value.getDays)), day)
      checkComparison(new DaysWritable(day, rebaseGregorianToJulianDays(day)), day)
    }
  }

  test("SPARK-61763: reused dates compare consistently with DateWritable") {
    val value = new DaysWritable()
    days.foreach { day =>
      value.set(day)
      checkComparison(value, day)
    }
  }

  test("SPARK-61763: deserialized dates compare consistently with DateWritable") {
    val restored = new DaysWritable()
    days.foreach { day =>
      val output = new DataOutputBuffer()
      new DaysWritable(day).write(output)
      val input = new DataInputBuffer()
      input.reset(output.getData, output.getLength)
      restored.readFields(input)
      checkComparison(restored, day)
    }
  }

  test("SPARK-61763: maps retain distinct date keys") {
    for (map <- Seq[JMap[DateWritable, Int]](
        new HashMap[DateWritable, Int](), new TreeMap[DateWritable, Int]())) {
      days.zipWithIndex.foreach { case (day, index) =>
        map.put(new DaysWritable(day), index)
      }
      assert(map.size() == days.size)
      days.zipWithIndex.foreach { case (day, index) =>
        assert(map.get(new DaysWritable(day)) == index)
        assert(map.get(new DateWritable(rebaseGregorianToJulianDays(day))) == index)
      }
    }
  }
}
