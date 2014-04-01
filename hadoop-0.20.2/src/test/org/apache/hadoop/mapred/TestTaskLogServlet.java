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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.testing.HttpTester;
import org.mortbay.jetty.testing.ServletTester;

public class TestTaskLogServlet {
  private static final Log LOG = LogFactory.getLog(TestTaskLogServlet.class);
  private ServletTester tester;

  @Before
  public void setup() throws Exception {
    tester = new ServletTester();
    tester.setContextPath("/");
    tester.addServlet(TaskLogServlet.class, "/tasklog");
    tester.start();
  }

  @Test
  public void testMissingParameters() throws Exception {
    HttpTester request = new HttpTester();
    request.setMethod("GET");
    request.setURI("/tasklog");
    request.setVersion("HTTP/1.0");
    
    HttpTester response = new HttpTester();
    response.parse(tester.getResponses(request.generate()));

    assertEquals(400,response.getStatus());
  }
  
  private void setupValidLogs(String attemptIdStr) throws IOException {
    TaskAttemptID attemptId = TaskAttemptID.forName(attemptIdStr);
    File logDir = TaskLog.getAttemptDir(attemptId, false);
    FileUtil.fullyDelete(logDir);
    logDir.mkdirs();
    assertTrue(logDir.exists());

    // Now make the logs with some HTML in the output
    TaskLog.syncLogs(logDir.getAbsolutePath(), attemptId, false, false);
    makeLog(new File(logDir, "stderr"), "<b>this is stderr");
    makeLog(new File(logDir, "stdout"), "<b>this is stdout");
    makeLog(new File(logDir, "syslog"), "<b>this is syslog");
    TaskLog.syncLogs(logDir.getAbsolutePath(), attemptId, false, false);
  }
  
  @Test
  public void testHtmlLogs() throws Exception {
    String attemptIdStr = "attempt_123_0001_m_000001_0";
    setupValidLogs(attemptIdStr);

    HttpTester request = new HttpTester();
    request.setMethod("GET");
    request.setURI("/tasklog?attemptid=" + attemptIdStr);
    request.setVersion("HTTP/1.0");
    
    // Make sure all the contents show up and properly escaped
    HttpTester response = doRequest(request);
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    assertEquals("text/html; charset=utf-8", response.getHeader("content-type"));
    assertTrue(response.getContent().contains("&lt;b&gt;this is stderr"));
    assertTrue(response.getContent().contains("&lt;b&gt;this is stdout"));
    assertTrue(response.getContent().contains("&lt;b&gt;this is syslog"));
    
    // Only read a small chunk of each file <***b>thi***s
    // (should still be escaped)
    request.setURI("/tasklog?attemptid=" + attemptIdStr
        + "&start=1&end=6");
    response = doRequest(request);
    assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    assertEquals("text/html; charset=utf-8", response.getHeader("content-type"));
    assertFalse(response.getContent().contains("&lt;b"));
    assertFalse(response.getContent().contains("this is"));    
    assertTrue(response.getContent().contains("b&gt;thi</pre>"));
  }
  
  @Test
  public void testPlaintextLogs() throws Exception {
    String attemptIdStr = "attempt_123_0001_m_000001_0";
    setupValidLogs(attemptIdStr);

    HttpTester request = new HttpTester();
    request.setMethod("GET");
    request.setURI("/tasklog?plaintext=true&attemptid=" + attemptIdStr);
    request.setVersion("HTTP/1.0");
    
    // Make sure all the contents show up and properly escaped
    HttpTester response = doRequest(request);
    // Bad request because we require a 'filter'
    assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());

    // Try again with filter
    request.setURI("/tasklog?plaintext=true&filter=stdout&attemptid=" + attemptIdStr);
    response = doRequest(request);
    
    // Response should be text/plain, not be escaped
    assertEquals("text/plain; charset=utf-8", response.getHeader("content-type"));
    assertEquals("<b>this is stdout", response.getContent());
    
    // Test range request
    request.setURI("/tasklog?plaintext=true&filter=stdout" +
        "&attemptid=" + attemptIdStr +
        "&start=1&end=6");
    response = doRequest(request);
    
    // Response should be text/plain, not be escaped
    assertEquals("text/plain; charset=utf-8", response.getHeader("content-type"));
    assertEquals("b>thi", response.getContent());    
  }
  
  private HttpTester doRequest(HttpTester request) throws Exception {
    String reqStr = request.generate();
    LOG.info("Testing request: " + reqStr);
    String respStr = tester.getResponses(reqStr);
    LOG.info("Response: " + respStr);
    HttpTester response = new HttpTester();
    response.parse(respStr);
    return response;
  }
  
  private void makeLog(File f, String contents) throws IOException {
    LOG.info("Creating log at " + f);
    FileWriter fw = new FileWriter(f);
    try {
      fw.write(contents);
    } finally {
      fw.close();
    }
  }
  
}
