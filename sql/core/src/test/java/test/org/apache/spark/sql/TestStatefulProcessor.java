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

package test.org.apache.spark.sql;

import java.util.*;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.*;

public class TestStatefulProcessor extends StatefulProcessor<Integer, String, String> {

  private transient ValueState<Long> countState;

  @Override
  public void init(OutputMode outputMode, TimeoutMode timeoutMode) {
    countState = this.getHandle().getValueState("countState",
      Encoders.LONG());
  }

  @Override
  public scala.collection.Iterator<String> handleInputRows(
      Integer key,
      scala.collection.Iterator<String> rows,
      TimerValues timerValues,
      ExpiredTimerInfo expiredTimerInfo) {

    java.util.List<String> result = new ArrayList<>();
    if (!expiredTimerInfo.isValid()) {
      long count = 0;
      if (countState.exists()) {
        count = countState.get();
      }

      long numRows = 0;
      StringBuilder sb = new StringBuilder(key.toString());
      while (rows.hasNext()) {
        numRows++;
        sb.append(rows.next());
      }

      count += numRows;
      countState.update(count);
      assert (countState.get() == count);

      result.add(sb.toString());
    }
    return CollectionConverters.asScala(result).iterator();
  }
}
