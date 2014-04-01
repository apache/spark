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
package org.apache.hadoop.util;

import java.io.*;
import java.util.Calendar;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;

public class ServletUtil {
  /**
   * Initial HTML header
   */
  public static PrintWriter initHTML(ServletResponse response, String title
      ) throws IOException {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<html>\n"
        + "<link rel='stylesheet' type='text/css' href='/static/hadoop.css'>\n"
        + "<title>" + title + "</title>\n"
        + "<body>\n"
        + "<h1>" + title + "</h1>\n");
    return out;
  }

  /**
   * Get a parameter from a ServletRequest.
   * Return null if the parameter contains only white spaces.
   */
  public static String getParameter(ServletRequest request, String name) {
    String s = request.getParameter(name);
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.length() == 0? null: s;
  }

  public static final String HTML_TAIL = "<hr />\n"
    + "<a href=\"http://www.cloudera.com/hadoop/\">Cloudera's Distribution"
    + " including Apache Hadoop</a>, " + 
    + Calendar.getInstance().get(Calendar.YEAR) + ".\n"
    + "</body></html>";
  
  /**
   * HTML footer to be added in the jsps.
   * @return the HTML footer.
   */
  public static String htmlFooter() {
    return HTML_TAIL;
  }
  
  /**
   * Generate the percentage graph and returns HTML representation string
   * of the same.
   * 
   * @param perc The percentage value for which graph is to be generated
   * @param width The width of the display table
   * @return HTML String representation of the percentage graph
   * @throws IOException
   */
  public static String percentageGraph(int perc, int width) throws IOException {
    assert perc >= 0; assert perc <= 100;

    StringBuilder builder = new StringBuilder();

    builder.append("<table border=\"1px\" width=\""); builder.append(width);
    builder.append("px\"><tr>");
    if(perc > 0) {
      builder.append("<td cellspacing=\"0\" class=\"perc_filled\" width=\"");
      builder.append(perc); builder.append("%\"></td>");
    }if(perc < 100) {
      builder.append("<td cellspacing=\"0\" class=\"perc_nonfilled\" width=\"");
      builder.append(100 - perc); builder.append("%\"></td>");
    }
    builder.append("</tr></table>");
    return builder.toString();
  }
  
  /**
   * Generate the percentage graph and returns HTML representation string
   * of the same.
   * @param perc The percentage value for which graph is to be generated
   * @param width The width of the display table
   * @return HTML String representation of the percentage graph
   * @throws IOException
   */
  public static String percentageGraph(float perc, int width) throws IOException {
    return percentageGraph((int)perc, width);
  }

  /**
   * Escape and encode a string regarded as within the query component of an URI.
   * @param value the value to encode
   * @return encoded query, null if UTF-8 is not supported
   */
  public static String encodeQueryValue(final String value) {
    try {
      return URIUtil.encodeWithinQuery(value, "UTF-8");
    } catch (URIException e) {
      throw new AssertionError("JVM does not support UTF-8"); // should never happen!
    }
  }

  /**
   * Escape and encode a string regarded as the path component of an URI.
   * @param path the path component to encode
   * @return encoded path, null if UTF-8 is not supported
   */
  public static String encodePath(final String path) {
    try {
      return URIUtil.encodePath(path, "UTF-8");
    } catch (URIException e) {
      throw new AssertionError("JVM does not support UTF-8"); // should never happen!
    }
  }

  /**
   * Parse and decode the path component from the given request.
   * @param request Http request to parse
   * @param servletName the name of servlet that precedes the path
   * @return decoded path component, null if UTF-8 is not supported
   */
  public static String getDecodedPath(final HttpServletRequest request, String servletName) {
    try {
      return URIUtil.decode(getRawPath(request, servletName), "UTF-8");
    } catch (URIException e) {
      throw new AssertionError("JVM does not support UTF-8"); // should never happen!
    }
  }

  /**
   * Parse the path component from the given request and return w/o decoding.
   * @param request Http request to parse
   * @param servletName the name of servlet that precedes the path
   * @return path component, null if the default charset is not supported
   */
  public static String getRawPath(final HttpServletRequest request, String servletName) {
    return request.getRequestURI().substring(servletName.length());
  }
}
