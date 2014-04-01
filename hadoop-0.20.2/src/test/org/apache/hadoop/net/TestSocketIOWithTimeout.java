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
package org.apache.hadoop.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.Pipe;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This tests timout out from SocketInputStream and
 * SocketOutputStream using pipes.
 * 
 * Normal read and write using these streams are tested by pretty much
 * every DFS unit test.
 */
public class TestSocketIOWithTimeout {

  static Log LOG = LogFactory.getLog(TestSocketIOWithTimeout.class);
  
  private static int TIMEOUT = 1*1000; 
  private static String TEST_STRING = "1234567890";

  private void doIO(InputStream in, OutputStream out,
      int expectedTimeout) throws IOException {
    /* Keep on writing or reading until we get SocketTimeoutException.
     * It expects this exception to occur within 100 millis of TIMEOUT.
     */
    byte buf[] = new byte[4192];
    
    while (true) {
      long start = System.currentTimeMillis();
      try {
        if (in != null) {
          in.read(buf);
        } else {
          out.write(buf);
        }
      } catch (SocketTimeoutException e) {
        long diff = System.currentTimeMillis() - start;
        LOG.info("Got SocketTimeoutException as expected after " + 
                 diff + " millis : " + e.getMessage());
        assertTrue(Math.abs(expectedTimeout - diff) <=
          TestNetUtils.TIME_FUDGE_MILLIS);
        break;
      }
    }
  }

  /**
   * Just reads one byte from the input stream.
   */
  static class ReadThread extends Thread {
    private InputStream in;
    private volatile Throwable thrown = null;

    public ReadThread(InputStream in) {
      this.in = in;
    }
    public void run() {
      try {
        try {
          in.read();
        } catch (IOException e) {
          LOG.info("Got expection while reading as expected : " + 
                   e.getMessage());
          return;
        }
        assertTrue(false);
      } catch (Throwable t) {
        thrown = t;
      }
    }
  }

  @Test
  public void testSocketIOWithTimeout() throws Exception {
    
    // first open pipe:
    Pipe pipe = Pipe.open();
    Pipe.SourceChannel source = pipe.source();
    Pipe.SinkChannel sink = pipe.sink();
    
    try {
      InputStream in = new SocketInputStream(source, TIMEOUT);
      OutputStream out = new SocketOutputStream(sink, TIMEOUT);
      
      byte[] writeBytes = TEST_STRING.getBytes();
      byte[] readBytes = new byte[writeBytes.length];
      
      out.write(writeBytes);
      doIO(null, out, TIMEOUT);
      
      in.read(readBytes);
      assertTrue(Arrays.equals(writeBytes, readBytes));
      doIO(in, null, TIMEOUT);

      // Change timeout on the read side.
      ((SocketInputStream)in).setTimeout(TIMEOUT * 2);
      doIO(in, null, TIMEOUT * 2);
      
      /*
       * Use a large timeout and expect the thread to return quickly
       * upon interruption.
       */
      ((SocketInputStream)in).setTimeout(0);
      ReadThread thread = new ReadThread(in);
      thread.start();
      // If the thread is interrupted before it calls read()
      // then it throws ClosedByInterruptException due to
      // some Java quirk. Waiting for it to call read()
      // gets it into select(), so we get the expected
      // InterruptedIOException.
      Thread.sleep(1000);
      thread.interrupt();
      thread.join();
      if (thread.thrown != null) {
        throw new RuntimeException(thread.thrown);
      }

      //make sure the channels are still open
      assertTrue(source.isOpen());
      assertTrue(sink.isOpen());
      
      // Nevertheless, the output stream is closed, because
      // a partial write may have succeeded (see comment in
      // SocketOutputStream#write(byte[]), int, int)
      try {
        out.write(1);
        fail("Did not throw");
      } catch (IOException ioe) {
        assertTrue(ioe.getMessage().contains("stream is closed"));
      }
      
      out.close();
      assertFalse(sink.isOpen());
      
      // close sink and expect -1 from source.read()
      assertEquals(-1, in.read());
      
      // make sure close() closes the underlying channel.
      in.close();
      assertFalse(source.isOpen());
      
    } finally {
      if (source != null) {
        source.close();
      }
      if (sink != null) {
        sink.close();
      }
    }
  }
}
