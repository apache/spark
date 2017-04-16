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
package org.apache.hadoop.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.mortbay.util.ajax.JSON;

public class TestMetricsServlet extends TestCase {
  MetricsContext nc1;
  MetricsContext nc2;
  // List containing nc1 and nc2.
  List<MetricsContext> contexts;
  OutputRecord outputRecord;
  
  /**
   * Initializes, for testing, two NoEmitMetricsContext's, and adds one value 
   * to the first of them.
   */
  public void setUp() throws IOException {
    nc1 = new NoEmitMetricsContext();
    nc1.init("test1", ContextFactory.getFactory());
    nc2 = new NoEmitMetricsContext();
    nc2.init("test2", ContextFactory.getFactory());
    contexts = new ArrayList<MetricsContext>();
    contexts.add(nc1);
    contexts.add(nc2);

    MetricsRecord r = nc1.createRecord("testRecord");
    
    r.setTag("testTag1", "testTagValue1");
    r.setTag("testTag2", "testTagValue2");
    r.setMetric("testMetric1", 1);
    r.setMetric("testMetric2", 33);
    r.update();

    Map<String, Collection<OutputRecord>> m = nc1.getAllRecords();
    assertEquals(1, m.size());
    assertEquals(1, m.values().size());
    Collection<OutputRecord> outputRecords = m.values().iterator().next();
    assertEquals(1, outputRecords.size());
    outputRecord = outputRecords.iterator().next();
  }
  
 
  
  public void testTagsMetricsPair() throws IOException {
    TagsMetricsPair pair = new TagsMetricsPair(outputRecord.getTagsCopy(), 
        outputRecord.getMetricsCopy());
    String s = JSON.toString(pair);
    assertEquals(
        "[{\"testTag1\":\"testTagValue1\",\"testTag2\":\"testTagValue2\"},"+
        "{\"testMetric1\":1,\"testMetric2\":33}]", s);
  }
  
  public void testGetMap() throws IOException {
    MetricsServlet servlet = new MetricsServlet();
    Map<String, Map<String, List<TagsMetricsPair>>> m = servlet.makeMap(contexts);
    assertEquals("Map missing contexts", 2, m.size());
    assertTrue(m.containsKey("test1"));
   
    Map<String, List<TagsMetricsPair>> m2 = m.get("test1");
    
    assertEquals("Missing records", 1, m2.size());
    assertTrue(m2.containsKey("testRecord"));
    assertEquals("Wrong number of tags-values pairs.", 1, m2.get("testRecord").size());
  }
  
  public void testPrintMap() throws IOException {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);
    MetricsServlet servlet = new MetricsServlet();
    servlet.printMap(out, servlet.makeMap(contexts));
    
    String EXPECTED = "" +
      "test1\n" +
      "  testRecord\n" +
      "    {testTag1=testTagValue1,testTag2=testTagValue2}:\n" +
      "      testMetric1=1\n" +
      "      testMetric2=33\n" +
      "test2\n";
    assertEquals(EXPECTED, sw.toString());
  }
}
