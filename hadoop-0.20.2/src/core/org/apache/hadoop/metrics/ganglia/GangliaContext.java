/*
 * GangliaContext.java
 *
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

package org.apache.hadoop.metrics.ganglia;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.Util;

/**
 * Context for sending metrics to Ganglia.
 * 
 */
public class GangliaContext extends AbstractMetricsContext {
    
  private static final String PERIOD_PROPERTY = "period";
  private static final String SERVERS_PROPERTY = "servers";
  private static final String UNITS_PROPERTY = "units";
  private static final String SLOPE_PROPERTY = "slope";
  private static final String TMAX_PROPERTY = "tmax";
  private static final String DMAX_PROPERTY = "dmax";
    
  private static final String DEFAULT_UNITS = "";
  private static final String DEFAULT_SLOPE = "both";
  private static final int DEFAULT_TMAX = 60;
  private static final int DEFAULT_DMAX = 0;
  private static final int DEFAULT_PORT = 8649;
  private static final int BUFFER_SIZE = 1500;       // as per libgmond.c

  private final Log LOG = LogFactory.getLog(this.getClass());    

  private static final Map<Class,String> typeTable = new HashMap<Class,String>(5);
    
  static {
    typeTable.put(String.class, "string");
    typeTable.put(Byte.class, "int8");
    typeTable.put(Short.class, "int16");
    typeTable.put(Integer.class, "int32");
    typeTable.put(Long.class, "float");
    typeTable.put(Float.class, "float");
  }
    
  protected byte[] buffer = new byte[BUFFER_SIZE];
  protected int offset;
    
  protected List<? extends SocketAddress> metricsServers;
  private Map<String,String> unitsTable;
  private Map<String,String> slopeTable;
  private Map<String,String> tmaxTable;
  private Map<String,String> dmaxTable;
    
  protected DatagramSocket datagramSocket;
    
  /** Creates a new instance of GangliaContext */
  public GangliaContext() {
  }
    
  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
    parseAndSetPeriod(PERIOD_PROPERTY);
        
    metricsServers = 
      Util.parse(getAttribute(SERVERS_PROPERTY), DEFAULT_PORT); 
        
    unitsTable = getAttributeTable(UNITS_PROPERTY);
    slopeTable = getAttributeTable(SLOPE_PROPERTY);
    tmaxTable  = getAttributeTable(TMAX_PROPERTY);
    dmaxTable  = getAttributeTable(DMAX_PROPERTY);
        
    try {
      datagramSocket = new DatagramSocket();
    }
    catch (SocketException se) {
      se.printStackTrace();
    }
  }

  public void emitRecord(String contextName, String recordName,
    OutputRecord outRec) 
  throws IOException {
    // Setup so that the records have the proper leader names so they are
    // unambiguous at the ganglia level, and this prevents a lot of rework
    StringBuilder sb = new StringBuilder();
    sb.append(contextName);
    sb.append('.');

    if (contextName.equals("jvm") && outRec.getTag("processName") != null) {
      sb.append(outRec.getTag("processName"));
      sb.append('.');
    }

    sb.append(recordName);
    sb.append('.');
    int sbBaseLen = sb.length();

    // emit each metric in turn
    for (String metricName : outRec.getMetricNames()) {
      Object metric = outRec.getMetric(metricName);
      String type = typeTable.get(metric.getClass());
      if (type != null) {
        sb.append(metricName);
        emitMetric(sb.toString(), type, metric.toString());
        sb.setLength(sbBaseLen);
      } else {
        LOG.warn("Unknown metrics type: " + metric.getClass());
      }
    }
  }
    
  protected void emitMetric(String name, String type,  String value) 
  throws IOException {
    String units = getUnits(name);
    int slope = getSlope(name);
    int tmax = getTmax(name);
    int dmax = getDmax(name);
        
    offset = 0;
    xdr_int(0);             // metric_user_defined
    xdr_string(type);
    xdr_string(name);
    xdr_string(value);
    xdr_string(units);
    xdr_int(slope);
    xdr_int(tmax);
    xdr_int(dmax);
        
    for (SocketAddress socketAddress : metricsServers) {
      DatagramPacket packet = 
        new DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }
  }
    
  protected String getUnits(String metricName) {
    String result = unitsTable.get(metricName);
    if (result == null) {
      result = DEFAULT_UNITS;
    }
    return result;
  }
    
  protected int getSlope(String metricName) {
    String slopeString = slopeTable.get(metricName);
    if (slopeString == null) {
      slopeString = DEFAULT_SLOPE; 
    }
    return ("zero".equals(slopeString) ? 0 : 3); // see gmetric.c
  }
    
  protected int getTmax(String metricName) {
    if (tmaxTable == null) {
      return DEFAULT_TMAX;
    }
    if (tmaxTable == null) {
      return DEFAULT_TMAX;
    }
    String tmaxString = tmaxTable.get(metricName);
    if (tmaxString == null) {
      return DEFAULT_TMAX;
    }
    else {
      return Integer.parseInt(tmaxString);
    }
  }
    
  protected int getDmax(String metricName) {
    String dmaxString = dmaxTable.get(metricName);
    if (dmaxString == null) {
      return DEFAULT_DMAX;
    }
    else {
      return Integer.parseInt(dmaxString);
    }
  }
    
  /**
   * Puts a string into the buffer by first writing the size of the string
   * as an int, followed by the bytes of the string, padded if necessary to
   * a multiple of 4.
   */
  protected void xdr_string(String s) {
    byte[] bytes = s.getBytes();
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  /**
   * Pads the buffer with zero bytes up to the nearest multiple of 4.
   */
  private void pad() {
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }
        
  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   */
  protected void xdr_int(int i) {
    buffer[offset++] = (byte)((i >> 24) & 0xff);
    buffer[offset++] = (byte)((i >> 16) & 0xff);
    buffer[offset++] = (byte)((i >> 8) & 0xff);
    buffer[offset++] = (byte)(i & 0xff);
  }
}
