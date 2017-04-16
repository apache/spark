/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.StatisticsCollector.TimeWindow;
import org.apache.hadoop.mapred.StatisticsCollector.Stat;

public class TestStatisticsCollector extends TestCase{

  public void testMovingWindow() throws Exception {
    StatisticsCollector collector = new StatisticsCollector(1);
    TimeWindow window = new TimeWindow("test", 6, 2);
    TimeWindow sincStart = StatisticsCollector.SINCE_START;
    TimeWindow[] windows = {sincStart, window};
    
    Stat stat = collector.createStat("m1", windows);
    
    stat.inc(3);
    collector.update();
    assertEquals(0, stat.getValues().get(window).getValue());
    assertEquals(3, stat.getValues().get(sincStart).getValue());
    
    stat.inc(3);
    collector.update();
    assertEquals((3+3), stat.getValues().get(window).getValue());
    assertEquals(6, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    assertEquals((3+3), stat.getValues().get(window).getValue());
    assertEquals(16, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    assertEquals((3+3+10+10), stat.getValues().get(window).getValue());
    assertEquals(26, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    stat.inc(10);
    collector.update();
    assertEquals((3+3+10+10+10+10), stat.getValues().get(window).getValue());
    assertEquals(46, stat.getValues().get(sincStart).getValue());
    
    stat.inc(10);
    collector.update();
    assertEquals((3+3+10+10+10+10), stat.getValues().get(window).getValue());
    assertEquals(56, stat.getValues().get(sincStart).getValue());
    
    stat.inc(12);
    collector.update();
    assertEquals((10+10+10+10+10+12), stat.getValues().get(window).getValue());
    assertEquals(68, stat.getValues().get(sincStart).getValue());
    
    stat.inc(13);
    collector.update();
    assertEquals((10+10+10+10+10+12), stat.getValues().get(window).getValue());
    assertEquals(81, stat.getValues().get(sincStart).getValue());
    
    stat.inc(14);
    collector.update();
    assertEquals((10+10+10+12+13+14), stat.getValues().get(window).getValue());
    assertEquals(95, stat.getValues().get(sincStart).getValue());
  }

}
