/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * this class is a time class to 
 * measure to measure the time 
 * taken for some event.
 */
public  class Timer {
  long startTimeEpoch;
  long finishTimeEpoch;
  private DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  public void startTime() throws IOException {
      startTimeEpoch = System.currentTimeMillis();
    }

    public void stopTime() throws IOException {
      finishTimeEpoch = System.currentTimeMillis();
    }

    public long getIntervalMillis() throws IOException {
      return finishTimeEpoch - startTimeEpoch;
    }
  
    public void printlnWithTimestamp(String message) throws IOException {
      System.out.println(formatCurrentTime() + "  " + message);
    }
  
    public String formatTime(long millis) {
      return formatter.format(millis);
    }
    
    public String getIntervalString() throws IOException {
      long time = getIntervalMillis();
      return formatTime(time);
    }
    
    public String formatCurrentTime() {
      return formatTime(System.currentTimeMillis());
    }

}

