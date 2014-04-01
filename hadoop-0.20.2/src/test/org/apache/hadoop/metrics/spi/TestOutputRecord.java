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
package org.apache.hadoop.metrics.spi;

import org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;

import junit.framework.TestCase;

public class TestOutputRecord extends TestCase {
  public void testCopy() {
    TagMap tags = new TagMap();
    tags.put("tagkey", "tagval");
    MetricMap metrics = new MetricMap();
    metrics.put("metrickey", 123.4);
    OutputRecord r = new OutputRecord(tags, metrics);
    
    assertEquals(tags, r.getTagsCopy());    
    assertNotSame(tags, r.getTagsCopy());
    assertEquals(metrics, r.getMetricsCopy());
    assertNotSame(metrics, r.getMetricsCopy());
  } 
}
