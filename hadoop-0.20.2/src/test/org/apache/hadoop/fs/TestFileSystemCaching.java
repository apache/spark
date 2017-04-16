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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import java.util.concurrent.Semaphore;

import junit.framework.TestCase;
import static junit.framework.Assert.assertTrue;

public class TestFileSystemCaching extends TestCase {

  public static class InitializeForeverFileSystem extends LocalFileSystem {
    final static Semaphore sem = new Semaphore(0);
    public void initialize(URI uri, Configuration conf) throws IOException {
      // notify that InitializeForeverFileSystem started initialization
      sem.release();
      try {
        while (true) {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }
  
  public void testCacheEnabledWithInitializeForeverFS() throws Exception {
    final Configuration conf = new Configuration();
    Thread t = new Thread() {
      public void run() {
        conf.set("fs.localfs1.impl", "org.apache.hadoop.fs." +
         "TestFileSystemCaching$InitializeForeverFileSystem");
        try {
          FileSystem.get(new URI("localfs1://a"), conf);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (URISyntaxException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
    // wait for InitializeForeverFileSystem to start initialization
    InitializeForeverFileSystem.sem.acquire();
    
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    FileSystem.get(new URI("cachedfile://a"), conf);
    t.interrupt();
    t.join();
  }

  public void testFsUniqueness() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    // multiple invocations of FileSystem.get return the same object.
    FileSystem fs1 = FileSystem.get(conf);
    FileSystem fs2 = FileSystem.get(conf);
    assertTrue(fs1 == fs2);

    // multiple invocations of FileSystem.newInstance return different objects
    fs1 = FileSystem.newInstance(new URI("cachedfile://a"), conf, "bar");
    fs2 = FileSystem.newInstance(new URI("cachedfile://a"), conf, "bar");
    assertTrue(fs1 != fs2 && !fs1.equals(fs2));
    fs1.close();
    fs2.close();
  }
}
