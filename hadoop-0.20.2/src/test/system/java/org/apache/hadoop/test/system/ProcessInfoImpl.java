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

package org.apache.hadoop.test.system;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ProcessInfoImpl implements ProcessInfo {

  private int threadCount;
  private long currentTime;
  private long freemem;
  private long maxmem;
  private long totmem;
  private Map<String, String> env;
  private Map<String, String> props;

  public ProcessInfoImpl() {
    env = new HashMap<String, String>();
    props = new HashMap<String, String>();
  }

  /**
   * Construct a concrete process information object. <br/>
   * 
   * @param threadCount
   *          count of threads.
   * @param currentTime
   * @param freememory
   * @param maximummemory
   * @param totalmemory
   * @param env
   *          environment list.
   */
  public ProcessInfoImpl(int threadCount, long currentTime, long freemem,
      long maxmem, long totmem, Map<String, String> env, 
      Map<String, String> props) {
    this.threadCount = threadCount;
    this.currentTime = currentTime;
    this.freemem = freemem;
    this.maxmem = maxmem;
    this.totmem = totmem;
    this.env = env;
    this.props = props;
  }

  @Override
  public int activeThreadCount() {
    return threadCount;
  }

  @Override
  public long currentTimeMillis() {
    return currentTime;
  }

  @Override
  public long freeMemory() {
    return freemem;
  }

  @Override
  public Map<String, String> getEnv() {
    return env;
  }

  @Override
  public Map<String,String> getSystemProperties() {
    return props;
  }

  @Override
  public long maxMemory() {
    return maxmem;
  }

  @Override
  public long totalMemory() {
    return totmem;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.threadCount = in.readInt();
    this.currentTime = in.readLong();
    this.freemem = in.readLong();
    this.maxmem = in.readLong();
    this.totmem = in.readLong();
    read(in, env);
    read(in, props);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(threadCount);
    out.writeLong(currentTime);
    out.writeLong(freemem);
    out.writeLong(maxmem);
    out.writeLong(totmem);
    write(out, env);
    write(out, props);
  }

  private void read(DataInput in, Map<String, String> map) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i = i + 2) {
      String key = in.readUTF();
      String value = in.readUTF();
      map.put(key, value);
    }
  }

  private void write(DataOutput out, Map<String, String> map) 
  throws IOException {
    int size = (map.size() * 2);
    out.writeInt(size);
    for (Map.Entry<String, String> entry : map.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
  }

  @Override
  public String toString() {
    StringBuffer strBuf = new StringBuffer();
    strBuf.append(String.format("active threads : %d\n", threadCount));
    strBuf.append(String.format("current time  : %d\n", currentTime));
    strBuf.append(String.format("free memory  : %d\n", freemem));
    strBuf.append(String.format("total memory  : %d\n", totmem));
    strBuf.append(String.format("max memory  : %d\n", maxmem));
    strBuf.append("Environment Variables : \n");
    for (Map.Entry<String, String> entry : env.entrySet()) {
      strBuf.append(String.format("key : %s value : %s \n", entry.getKey(),
          entry.getValue()));
    }
    return strBuf.toString();
  }

}
