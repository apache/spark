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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.mockito.Mockito;
import org.mortbay.jetty.InclusiveByteRange;

/*
 * Mock input stream class that always outputs the current position of the stream. 
 */
class MockFSInputStream extends FSInputStream {
  long currentPos = 0;
  public int read() throws IOException {
    return (int)(currentPos++);
  }

  public void close() throws IOException {
  }

  public void seek(long pos) throws IOException {
    currentPos = pos;
  }
  
  public long getPos() throws IOException {
    return currentPos;
  }
  
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}


class MockHttpServletResponse implements HttpServletResponse {

  private int status = -1;
  
  public MockHttpServletResponse() {
  }
  
  public int getStatus() {
    return status;
  }
  
  
  public void setStatus(int sc) {
    status = sc;
  }
  
  public void setStatus(int sc, java.lang.String sm) {
  }
  
  public void addIntHeader(String name, int value) {
  }

  public void setIntHeader(String name, int value) { 
  }
  
  public void addHeader(String name, String value) {
  }

  public void setHeader(String name, String value) {
  }
  
  public void addDateHeader(java.lang.String name, long date) {
  }
  
  public void setDateHeader(java.lang.String name, long date) {
  }

  public void sendRedirect(java.lang.String location) { 
  }
  
  public void sendError(int e) {
  }
  
  public void sendError(int a, java.lang.String b) {
  }
  
  public String encodeRedirectUrl(java.lang.String a) {
    return null;
  }
  
  public String encodeUrl(java.lang.String url) {
    return null;
  }
  
  public String encodeRedirectURL(java.lang.String url) {
    return null;
  }
  
  public String encodeURL(java.lang.String url) {
    return null;
  }
  
  public boolean containsHeader(java.lang.String name) {
    return false;
  }
  
  public void addCookie(javax.servlet.http.Cookie cookie) {
  }
  
  public java.util.Locale getLocale() {
    return null;
  }
  
  public void setLocale(java.util.Locale loc) {
  }
  
  public void reset() {
  }
  
  public boolean isCommitted() {
    return false;
  }
  
  public void resetBuffer() {
  }
  
  public void flushBuffer() {
  }
  
  public int getBufferSize() {
    return 0;
  }
  
  public void setBufferSize(int size) {
  }
  
  public void setContentType(java.lang.String type) {
  }
  
  public void setContentLength(int len) {
  }
  
  public void setCharacterEncoding(java.lang.String charset) {
  }
  
  public java.io.PrintWriter getWriter() {
    return null;
  }
  
  public javax.servlet.ServletOutputStream getOutputStream() {
    return null;
  }
  
  public java.lang.String getContentType() {
    return null;
  }
  
  public java.lang.String getCharacterEncoding() {
    return null;
  }
}


public class TestStreamFile {
  private Configuration CONF = new Configuration();
  private DFSClient clientMock = Mockito.mock(DFSClient.class);
  private HttpServletRequest mockHttpServletRequest = 
    Mockito.mock(HttpServletRequest.class);
  private HttpServletResponse mockHttpServletResponse = 
    Mockito.mock(HttpServletResponse.class);
  private final ServletContext mockServletContext = 
    Mockito.mock(ServletContext.class);

  StreamFile sfile = new StreamFile() {
    private static final long serialVersionUID = -5513776238875189473L;
  
    public ServletContext getServletContext() {
      return mockServletContext;
    }
  
    @Override
    protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException, InterruptedException {
      return clientMock;
    }
  };
     
  // return an array matching the output of mockfsinputstream
  private static byte[] getOutputArray(int start, int count) {
    byte[] a = new byte[count];
    
    for (int i = 0; i < count; i++) {
      a[i] = (byte)(start+i);
    }

    return a;
  }
  
  @Test
  public void testWriteTo() throws IOException, InterruptedException {

    FSInputStream fsin = new MockFSInputStream();
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // new int[]{s_1, c_1, s_2, c_2, ..., s_n, c_n} means to test
    // reading c_i bytes starting at s_i
    int[] pairs = new int[]{ 0, 10000,
                             50, 100,
                             50, 6000,
                             1000, 2000,
                             0, 1,
                             0, 0,
                             5000, 0,
                            };
                            
    assertTrue("Pairs array must be even", pairs.length % 2 == 0);
    
    for (int i = 0; i < pairs.length; i+=2) {
      StreamFile.copyFromOffset(fsin, os, pairs[i], pairs[i+1]);
      assertArrayEquals("Reading " + pairs[i+1]
                        + " bytes from offset " + pairs[i],
                        getOutputArray(pairs[i], pairs[i+1]),
                        os.toByteArray());
      os.reset();
    }
    
  }

  @SuppressWarnings("unchecked")
  private List<InclusiveByteRange> strToRanges(String s, int contentLength) {
    List<String> l = Arrays.asList(new String[]{"bytes="+s});
    Enumeration<String> e = (new Vector<String>(l)).elements();
    return InclusiveByteRange.satisfiableRanges(e, contentLength);
  }
  
  @Test
  public void testSendPartialData() throws IOException, InterruptedException {
    FSInputStream in = new MockFSInputStream();
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // test if multiple ranges, then 416
    { 
      List<InclusiveByteRange> ranges = strToRanges("0-,10-300", 500);
      MockHttpServletResponse response = new MockHttpServletResponse();
      StreamFile.sendPartialData(in, os, response, 500, ranges);
      assertEquals("Multiple ranges should result in a 416 error",
                   416, response.getStatus());
    }
                              
    // test if no ranges, then 416
    { 
      os.reset();
      MockHttpServletResponse response = new MockHttpServletResponse();
      StreamFile.sendPartialData(in, os, response, 500, null);
      assertEquals("No ranges should result in a 416 error",
                   416, response.getStatus());
    }

    // test if invalid single range (out of bounds), then 416
    { 
      List<InclusiveByteRange> ranges = strToRanges("600-800", 500);
      MockHttpServletResponse response = new MockHttpServletResponse();
      StreamFile.sendPartialData(in, os, response, 500, ranges);
      assertEquals("Single (but invalid) range should result in a 416",
                   416, response.getStatus());
    }

      
    // test if one (valid) range, then 206
    { 
      List<InclusiveByteRange> ranges = strToRanges("100-300", 500);
      MockHttpServletResponse response = new MockHttpServletResponse();
      StreamFile.sendPartialData(in, os, response, 500, ranges);
      assertEquals("Single (valid) range should result in a 206",
                   206, response.getStatus());
      assertArrayEquals("Byte range from 100-300",
                        getOutputArray(100, 201),
                        os.toByteArray());
    }
    
  }
  
  
  // Test for positive scenario
  @Test
  public void testDoGetShouldWriteTheFileContentIntoServletOutputStream()
      throws Exception {

    MiniDFSCluster cluster = new MiniDFSCluster(CONF, 1, true, null);
    try {
      Path testFile = createFile();
      setUpForDoGetTest(cluster, testFile);
      ServletOutputStreamExtn outStream = new ServletOutputStreamExtn();
      Mockito.doReturn(outStream).when(mockHttpServletResponse)
          .getOutputStream();
      StreamFile sfile = new StreamFile() {

        private static final long serialVersionUID = 7715590481809562722L;

        public ServletContext getServletContext() {
          return mockServletContext;
        }
      };
      StreamFile.nameNodeAddr = NameNode.getServiceAddress(CONF, true);
      sfile.doGet(mockHttpServletRequest, mockHttpServletResponse);
      assertEquals("Not writing the file data into ServletOutputStream",
          outStream.getResult(), "test");
    } finally {
      cluster.shutdown();
    }
  }

  // Test for cleaning the streams in exception cases also
  @Test
  public void testDoGetShouldCloseTheDFSInputStreamIfResponseGetOutPutStreamThrowsAnyException()
      throws Exception {

    MiniDFSCluster cluster = new MiniDFSCluster(CONF, 1, true, null);
    try {
      Path testFile = createFile();

      setUpForDoGetTest(cluster, testFile);

      Mockito.doThrow(new IOException()).when(mockHttpServletResponse)
          .getOutputStream();
      DFSInputStream fsMock = Mockito.mock(DFSInputStream.class);

      Mockito.doReturn(fsMock).when(clientMock).open(testFile.toString());

      Mockito.doReturn(Long.valueOf(4)).when(fsMock).getFileLength();

      try {
        sfile.doGet(mockHttpServletRequest, mockHttpServletResponse);
        fail("Not throwing the IOException");
      } catch (IOException e) {
        Mockito.verify(clientMock, Mockito.atLeastOnce()).close();
      }

    } finally {
      cluster.shutdown();
    }
  }

  private void setUpForDoGetTest(MiniDFSCluster cluster, Path testFile)
      throws IOException {

    Mockito.doReturn(CONF).when(mockServletContext).getAttribute(
        JspHelper.CURRENT_CONF);
    Mockito.doReturn(testFile.toString()).when(mockHttpServletRequest)
      .getParameter("filename");
    Mockito.doReturn("/streamFile"+testFile.toString()).when(mockHttpServletRequest)
      .getRequestURI();
  }

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    try {
      out.writeBytes("test");
    } finally {
      out.close();
    }
    assertTrue(fs.exists(f));
    return f;
  }

  private Path createFile() throws IOException {
    FileSystem fs = FileSystem.get(CONF);
    Path testFile = new Path("/test/mkdirs/doGet");
    writeFile(fs, testFile);
    return testFile;
  }

  public static class ServletOutputStreamExtn extends ServletOutputStream {
    private StringBuffer buffer = new StringBuffer(3);

    public String getResult() {
      return buffer.toString();
    }

    @Override
    public void write(int b) throws IOException {
      buffer.append((char) b);
    }
  }
}
