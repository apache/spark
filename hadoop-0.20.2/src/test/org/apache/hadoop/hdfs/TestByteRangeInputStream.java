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
package org.apache.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.hdfs.ByteRangeInputStream;
import org.apache.hadoop.hdfs.ByteRangeInputStream.URLOpener;

import org.junit.Test;
import static org.junit.Assert.*;

class MockHttpURLConnection extends HttpURLConnection {
  MockURL m;
  
  public MockHttpURLConnection(URL u, MockURL m) {
    super(u); 
    this.m = m;
  }
  
  public boolean usingProxy(){
    return false;
  }
  
  public void disconnect() {
  }
  
  public void connect() throws IOException {
    m.setMsg("Connect: "+url+", Range: "+getRequestProperty("Range"));
  }
  
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream("asdf".getBytes());
  } 

  public URL getURL() {
    URL u = null;
    try {
      u = new URL("http://resolvedurl/");
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return u;
  }
  
  public int getResponseCode() {
    if (m.responseCode != -1) {
      return m.responseCode;
    } else {
      if (getRequestProperty("Range") == null) {
        return 200;
      } else {
        return 206;
      }
    }
  }
  
}

class MockURL extends URLOpener {
  String msg;
  public int responseCode = -1;
  
  public MockURL(URL u) {
    super(u);
  }

  public MockURL(String s) throws MalformedURLException {
    this(new URL(s));
  }

  public HttpURLConnection openConnection() throws IOException {
    return new MockHttpURLConnection(url, this);
  }    

  public void setMsg(String s) {
    msg = s;
  }
  
  public String getMsg() {
    return msg;
  }
}

public class TestByteRangeInputStream {

  @Test
  public void testByteRange() throws IOException, InterruptedException {
    MockURL o = new MockURL("http://test/");
    MockURL r =  new MockURL((URL)null);
    ByteRangeInputStream is = new ByteRangeInputStream(o, r);

    assertEquals("getPos wrong", 0, is.getPos());

    is.read();

    assertEquals("Initial call made incorrectly", 
                 "Connect: http://test/, Range: null",
                 o.getMsg());

    assertEquals("getPos should be 1 after reading one byte", 1, is.getPos());

    o.setMsg(null);

    is.read();

    assertEquals("getPos should be 2 after reading two bytes", 2, is.getPos());

    assertNull("No additional connections should have been made (no seek)",
               o.getMsg());

    r.setMsg(null);
    r.setURL(new URL("http://resolvedurl/"));
    
    is.seek(100);
    is.read();

    assertEquals("Seek to 100 bytes made incorrectly", 
                 "Connect: http://resolvedurl/, Range: bytes=100-",
                 r.getMsg());

    assertEquals("getPos should be 101 after reading one byte", 101, is.getPos());

    r.setMsg(null);

    is.seek(101);
    is.read();

    assertNull("Seek to 101 should not result in another request", r.getMsg());

    r.setMsg(null);
    is.seek(2500);
    is.read();

    assertEquals("Seek to 2500 bytes made incorrectly", 
                 "Connect: http://resolvedurl/, Range: bytes=2500-",
                 r.getMsg());

    r.responseCode = 200;
    is.seek(500);
    
    try {
      is.read();
      fail("Exception should be thrown when 200 response is given "
           + "but 206 is expected");
    } catch (IOException e) {
      assertEquals("Should fail because incorrect response code was sent",
                   "HTTP_PARTIAL expected, received 200", e.getMessage());
    }

    r.responseCode = 206;
    is.seek(0);

    try {
      is.read();
      fail("Exception should be thrown when 206 response is given "
           + "but 200 is expected");
    } catch (IOException e) {
      assertEquals("Should fail because incorrect response code was sent",
                   "HTTP_OK expected, received 206", e.getMessage());
    }
  }
}
