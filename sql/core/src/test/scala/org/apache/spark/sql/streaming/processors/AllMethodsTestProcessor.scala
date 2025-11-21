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
package org.apache.spark.sql.streaming.processors

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{ListState, MapState, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}

/** Test processor that exercises all state methods for coverage testing. */
class AllMethodsTestProcessor extends StatefulProcessor[String, String, (String, String)] {

  @transient private var valueState: ValueState[Int] = _
  @transient private var listState: ListState[String] = _
  @transient private var mapState: MapState[String, Int] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    valueState = getHandle.getValueState[Int]("value", Encoders.scalaInt, TTLConfig.NONE)
    listState = getHandle.getListState[String]("list", Encoders.STRING, TTLConfig.NONE)
    mapState =
      getHandle.getMapState[String, Int]("map", Encoders.STRING, Encoders.scalaInt, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues
  ): Iterator[(String, String)] = {
    val results = scala.collection.mutable.ArrayBuffer[(String, String)]()

    inputRows.foreach { cmd =>
      cmd match {
        case "value-exists" =>
          results += ((key, s"value-exists:${valueState.exists()}"))
        case "value-set" =>
          valueState.update(42)
          results += ((key, "value-set:done"))
        case "value-clear" =>
          valueState.clear()
          results += ((key, "value-clear:done"))
        case "list-exists" =>
          results += ((key, s"list-exists:${listState.exists()}"))
        case "list-append" =>
          listState.appendValue("a")
          listState.appendValue("b")
          results += ((key, "list-append:done"))
        case "list-append-array" =>
          listState.appendList(Array("c", "d"))
          results += ((key, "list-append-array:done"))
        case "list-get" =>
          val items = listState.get().toList.mkString(",")
          results += ((key, s"list-get:$items"))
        case "map-exists" =>
          results += ((key, s"map-exists:${mapState.exists()}"))
        case "map-add" =>
          mapState.updateValue("x", 1)
          mapState.updateValue("y", 2)
          mapState.updateValue("z", 3)
          results += ((key, "map-add:done"))
        case "map-keys" =>
          val keys = mapState.keys().toList.sorted.mkString(",")
          results += ((key, s"map-keys:$keys"))
        case "map-values" =>
          val values = mapState.values().toList.sorted.mkString(",")
          results += ((key, s"map-values:$values"))
        case "map-iterator" =>
          val pairs =
            mapState.iterator().toList.sortBy(_._1).map(p => s"${p._1}=${p._2}").mkString(",")
          results += ((key, s"map-iterator:$pairs"))
        case "map-remove" =>
          mapState.removeKey("y")
          results += ((key, "map-remove:done"))
        case "map-clear" =>
          mapState.clear()
          results += ((key, "map-clear:done"))
      }
    }

    results.iterator
  }
}
