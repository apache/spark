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

package org.apache.spark.deploy.yarn;

import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.EnumSet;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

// Class containing general purpose proxy utilities
//
// This class is copied from Hadoop 3.4.0
// org.apache.hadoop.yarn.server.webproxy.ProxyUtils
//
// Modification:
// Migrate from javax.servlet to jakarta.servlet
public class ProxyUtils {
  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(ProxyUtils.class);
  public static final String E_HTTP_HTTPS_ONLY =
      "This filter only works for HTTP/HTTPS";
  public static final String LOCATION = "Location";

  public static class __ implements Hamlet.__ {
    // Empty
  }

  public static class Page extends Hamlet {
    Page(PrintWriter out) {
      super(out, 0, false);
    }

    public HTML<ProxyUtils.__> html() {
      return new HTML<>("html", null, EnumSet.of(EOpt.ENDTAG));
    }
  }

  /**
   * Handle redirects with a status code that can in future support verbs other
   * than GET, thus supporting full REST functionality.
   * <p>
   * The target URL is included in the redirect text returned
   * <p>
   * At the end of this method, the output stream is closed.
   *
   * @param request request (hence: the verb and any other information
   * relevant to a redirect)
   * @param response the response
   * @param target the target URL -unencoded
   *
   */
  public static void sendRedirect(HttpServletRequest request,
      HttpServletResponse response,
      String target)
      throws IOException {
    LOG.debug("Redirecting {} {} to {}",
          request.getMethod(),
          request.getRequestURI(),
          target);
    String location = response.encodeRedirectURL(target);
    response.setStatus(HttpServletResponse.SC_FOUND);
    response.setHeader(LOCATION, location);
    response.setContentType(MimeType.HTML);
    PrintWriter writer = response.getWriter();
    Page p = new Page(writer);
    p.html()
      .head().title("Moved").__()
      .body()
      .h1("Moved")
      .div()
        .__("Content has moved ")
        .a(location, "here").__()
      .__().__();
    writer.close();
  }


  /**
   * Output 404 with appropriate message.
   * @param resp the http response.
   * @param message the message to include on the page.
   * @throws IOException on any error.
   */
  public static void notFound(HttpServletResponse resp, String message)
      throws IOException {
    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    resp.setContentType(MimeType.HTML);
    Page p = new Page(resp.getWriter());
    p.html().h1(message).__();
  }

  /**
   * Reject any request that isn't from an HTTP servlet
   * @param req request
   * @throws ServletException if the request is of the wrong type
   */
  public static void rejectNonHttpRequests(ServletRequest req) throws
      ServletException {
    if (!(req instanceof HttpServletRequest)) {
      throw new ServletException(E_HTTP_HTTPS_ONLY);
    }
  }
}
