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
package org.apache.hadoop.metrics.jvm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A log4J Appender that simply counts logging events in three levels:
 * fatal, error and warn.
 */
public class EventCounter extends AppenderSkeleton {
        
    private static final int FATAL = 0;
    private static final int ERROR = 1;
    private static final int WARN  = 2;
    private static final int INFO  = 3;
    
    private static class EventCounts {
        private final long[] counts = { 0, 0, 0, 0 };
    
        private synchronized void incr(int i) { 
            ++counts[i]; 
        }
        
        private synchronized long get(int i) { 
            return counts[i]; 
        }
    }
    private static EventCounts counts = new EventCounts();
    
    public static long getFatal() { 
        return counts.get(FATAL); 
    }
    
    public static long getError() { 
        return counts.get(ERROR); 
    }
    
    public static long getWarn() { 
        return counts.get(WARN);  
    }
    
    public static long getInfo() {
        return counts.get(INFO);
    }
    
    public void append(LoggingEvent event) {
        Level level = event.getLevel();
        if (level == Level.INFO) {
            counts.incr(INFO);
        }
        else if (level == Level.WARN) {
            counts.incr(WARN);
        }
        else if (level == Level.ERROR) {
            counts.incr(ERROR);
        }
        else if (level == Level.FATAL) {
            counts.incr(FATAL);
        }

    }
    
    // Strange: these two methods are abstract in AppenderSkeleton, but not
    // included in the javadoc (log4j 1.2.13).
    
    public void close() {
    }
    public boolean requiresLayout() {
        return false;
    }
    
    
    
}
