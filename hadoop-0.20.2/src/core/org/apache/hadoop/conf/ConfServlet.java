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
package org.apache.hadoop.conf;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.http.HttpServer;

/**
 * A servlet to print out the running configuration data.
 */
public class ConfServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private static final String FORMAT_JSON = "json";
  private static final String FORMAT_XML = "xml";
  private static final String FORMAT_PARAM = "format";

  /**
   * Return the Configuration of the daemon hosting this servlet.
   * This is populated when the HttpServer starts.
   */
  private Configuration getConfFromContext() {
    Configuration conf = (Configuration)getServletContext().getAttribute(
        HttpServer.CONF_CONTEXT_ATTRIBUTE);
    assert conf != null;
    return conf;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                   request, response)) {
      return;
    }
    String format = request.getParameter(FORMAT_PARAM);
    if (null == format) {
      format = FORMAT_XML;
    }

    if (FORMAT_XML.equals(format)) {
      response.setContentType("text/xml; charset=utf-8");
    } else if (FORMAT_JSON.equals(format)) {
      response.setContentType("application/json; charset=utf-8");
    }

    Writer out = response.getWriter();
    try {
      writeResponse(getConfFromContext(), out, format);
    } catch (BadFormatException bfe) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, bfe.getMessage());
    }
    out.close();
  }

  /**
   * Guts of the servlet - extracted for easy testing.
   */
  static void writeResponse(Configuration conf, Writer out, String format)
    throws IOException, BadFormatException {
    if (FORMAT_JSON.equals(format)) {
      Configuration.dumpConfiguration(conf, out);
    } else if (FORMAT_XML.equals(format)) {
      conf.writeXml(out);
    } else {
      throw new BadFormatException("Bad format: " + format);
    }
  }

  public static class BadFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    public BadFormatException(String msg) {
      super(msg);
    }
  }

}
