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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;
import org.mortbay.util.ajax.JSON;
import org.mortbay.util.ajax.JSON.Output;

/**
 * A servlet to print out metrics data.  By default, the servlet returns a 
 * textual representation (no promises are made for parseability), and
 * users can use "?format=json" for parseable output.
 */
public class MetricsServlet extends HttpServlet {
  
  /**
   * A helper class to hold a TagMap and MetricMap.
   */
  static class TagsMetricsPair implements JSON.Convertible {
    final TagMap tagMap;
    final MetricMap metricMap;
    
    public TagsMetricsPair(TagMap tagMap, MetricMap metricMap) {
      this.tagMap = tagMap;
      this.metricMap = metricMap;
    }

    @SuppressWarnings("unchecked")
    public void fromJSON(Map map) {
      throw new UnsupportedOperationException();
    }

    /** Converts to JSON by providing an array. */
    public void toJSON(Output out) {
      out.add(new Object[] { tagMap, metricMap });
    }
  }
  
  /**
   * Collects all metric data, and returns a map:
   *   contextName -> recordName -> [ (tag->tagValue), (metric->metricValue) ].
   * The values are either String or Number.  The final value is implemented
   * as a list of TagsMetricsPair.
   */
   Map<String, Map<String, List<TagsMetricsPair>>> makeMap(
       Collection<MetricsContext> contexts) throws IOException {
    Map<String, Map<String, List<TagsMetricsPair>>> map = 
      new TreeMap<String, Map<String, List<TagsMetricsPair>>>();

    for (MetricsContext context : contexts) {
      Map<String, List<TagsMetricsPair>> records = 
        new TreeMap<String, List<TagsMetricsPair>>();
      map.put(context.getContextName(), records);
    
      for (Map.Entry<String, Collection<OutputRecord>> r : 
          context.getAllRecords().entrySet()) {
        List<TagsMetricsPair> metricsAndTags = 
          new ArrayList<TagsMetricsPair>();
        records.put(r.getKey(), metricsAndTags);
        for (OutputRecord outputRecord : r.getValue()) {
          TagMap tagMap = outputRecord.getTagsCopy();
          MetricMap metricMap = outputRecord.getMetricsCopy();
          metricsAndTags.add(new TagsMetricsPair(tagMap, metricMap));
        }
      }
    }
    return map;
  }
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                   request, response)) {
      return;
    }
    String format = request.getParameter("format");
    Collection<MetricsContext> allContexts = 
      ContextFactory.getFactory().getAllContexts();
    if ("json".equals(format)) {
      response.setContentType("application/json; charset=utf-8");
      PrintWriter out = response.getWriter();
      try {
        // Uses Jetty's built-in JSON support to convert the map into JSON.
        out.print(new JSON().toJSON(makeMap(allContexts)));
      } finally {
        out.close();
      }
    } else {
      PrintWriter out = response.getWriter();
      try {
        printMap(out, makeMap(allContexts));
      } finally {
        out.close();
      }
    }
  }
  
  /**
   * Prints metrics data in a multi-line text form.
   */
  void printMap(PrintWriter out, Map<String, Map<String, List<TagsMetricsPair>>> map) {
    for (Map.Entry<String, Map<String, List<TagsMetricsPair>>> context : map.entrySet()) {
      out.println(context.getKey());
      for (Map.Entry<String, List<TagsMetricsPair>> record : context.getValue().entrySet()) {
        indent(out, 1);
        out.println(record.getKey());
        for (TagsMetricsPair pair : record.getValue()) {
          indent(out, 2);
          // Prints tag values in the form "{key=value,key=value}:"
          out.print("{");
          boolean first = true;
          for (Map.Entry<String, Object> tagValue : pair.tagMap.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.print(",");
            }
            out.print(tagValue.getKey());
            out.print("=");
            out.print(tagValue.getValue().toString());
          }
          out.println("}:");
          
          // Now print metric values, one per line
          for (Map.Entry<String, Number> metricValue : 
              pair.metricMap.entrySet()) {
            indent(out, 3);
            out.print(metricValue.getKey());
            out.print("=");
            out.println(metricValue.getValue().toString());
          }
        }
      }
    }    
  }
  
  private void indent(PrintWriter out, int indent) {
    for (int i = 0; i < indent; ++i) {
      out.append("  ");
    }
  }
}
