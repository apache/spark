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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.mapred.StatisticsCollector.Stat.TimeStat;

/**
 * Collects the statistics in time windows.
 */
class StatisticsCollector {

  private static final int DEFAULT_PERIOD = 5;

  static final TimeWindow 
    SINCE_START = new TimeWindow("Since Start", -1, -1);
  
  static final TimeWindow 
    LAST_WEEK = new TimeWindow("Last Week", 7 * 24 * 60 * 60, 60 * 60);
  
  static final TimeWindow 
    LAST_DAY = new TimeWindow("Last Day", 24 * 60 * 60, 60 * 60);
  
  static final TimeWindow 
    LAST_HOUR = new TimeWindow("Last Hour", 60 * 60, 60);
  
  static final TimeWindow 
    LAST_MINUTE = new TimeWindow("Last Minute", 60, 10);

  static final TimeWindow[] DEFAULT_COLLECT_WINDOWS = {
    StatisticsCollector.SINCE_START,
    StatisticsCollector.LAST_DAY,
    StatisticsCollector.LAST_HOUR
    };

  private final int period;
  private boolean started;
  
  private final Map<TimeWindow, StatUpdater> updaters = 
    new LinkedHashMap<TimeWindow, StatUpdater>();
  private final Map<String, Stat> statistics = new HashMap<String, Stat>();

  StatisticsCollector() {
    this(DEFAULT_PERIOD);
  }

  StatisticsCollector(int period) {
    this.period = period;
  }

  synchronized void start() {
    if (started) {
      return;
    }
    Timer timer = new Timer("Timer thread for monitoring ", true);
    TimerTask task = new TimerTask() {
      public void run() {
        update();
      }
    };
    long millis = period * 1000;
    timer.scheduleAtFixedRate(task, millis, millis);
    started = true;
  }

  protected synchronized void update() {
    for (StatUpdater c : updaters.values()) {
      c.update();
    }
  }

  Map<TimeWindow, StatUpdater> getUpdaters() {
    return Collections.unmodifiableMap(updaters);
  }

  Map<String, Stat> getStatistics() {
    return Collections.unmodifiableMap(statistics);
  }

  synchronized Stat createStat(String name) {
    return createStat(name, DEFAULT_COLLECT_WINDOWS);
  }

  synchronized Stat createStat(String name, TimeWindow[] windows) {
    if (statistics.get(name) != null) {
      throw new RuntimeException("Stat with name "+ name + 
          " is already defined");
    }
    Map<TimeWindow, TimeStat> timeStats = 
      new LinkedHashMap<TimeWindow, TimeStat>();
    for (TimeWindow window : windows) {
      StatUpdater collector = updaters.get(window);
      if (collector == null) {
        if(SINCE_START.equals(window)) {
          collector = new StatUpdater();
        } else {
          collector = new TimeWindowStatUpdater(window, period);
        }
        updaters.put(window, collector);
      }
      TimeStat timeStat = new TimeStat();
      collector.addTimeStat(name, timeStat);
      timeStats.put(window, timeStat);
    }

    Stat stat = new Stat(name, timeStats);
    statistics.put(name, stat);
    return stat;
  }

  synchronized Stat removeStat(String name) {
    Stat stat = statistics.remove(name);
    if (stat != null) {
      for (StatUpdater collector : updaters.values()) {
        collector.removeTimeStat(name);
      }
    }
    return stat;
  }

  static class TimeWindow {
    final String name;
    final int windowSize;
    final int updateGranularity;
    TimeWindow(String name, int windowSize, int updateGranularity) {
      if (updateGranularity > windowSize) {
        throw new RuntimeException(
            "Invalid TimeWindow: updateGranularity > windowSize");
      }
      this.name = name;
      this.windowSize = windowSize;
      this.updateGranularity = updateGranularity;
    }

    public int hashCode() {
      return name.hashCode() + updateGranularity + windowSize;
    }

    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final TimeWindow other = (TimeWindow) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (updateGranularity != other.updateGranularity)
        return false;
      if (windowSize != other.windowSize)
        return false;
      return true;
    }
  }

  static class Stat {
    final String name;
    private Map<TimeWindow, TimeStat> timeStats;

    private Stat(String name, Map<TimeWindow, TimeStat> timeStats) {
      this.name = name;
      this.timeStats = timeStats;
    }

    public synchronized void inc(int incr) {
      for (TimeStat ts : timeStats.values()) {
        ts.inc(incr);
      }
    }

    public synchronized void inc() {
      inc(1);
    }

    public synchronized Map<TimeWindow, TimeStat> getValues() {
      return Collections.unmodifiableMap(timeStats);
    }

    static class TimeStat {
      private final LinkedList<Integer> buckets = new LinkedList<Integer>();
      private int value;
      private int currentValue;

      public synchronized int getValue() {
        return value;
      }

      private synchronized void inc(int i) {
        currentValue += i;
      }

      private synchronized void addBucket() {
        buckets.addLast(currentValue);
        setValueToCurrent();
      }

      private synchronized void setValueToCurrent() {
        value += currentValue;
        currentValue = 0;
      }

      private synchronized void removeBucket() {
        int removed = buckets.removeFirst();
        value -= removed;
      }
    }
  }

  private static class StatUpdater {

    protected final Map<String, TimeStat> statToCollect = 
      new HashMap<String, TimeStat>();

    synchronized void addTimeStat(String name, TimeStat s) {
      statToCollect.put(name, s);
    }

    synchronized TimeStat removeTimeStat(String name) {
      return statToCollect.remove(name);
    }

    synchronized void update() {
      for (TimeStat stat : statToCollect.values()) {
        stat.setValueToCurrent();
      }
    }
  }

  /**
   * Updates TimeWindow statistics in buckets.
   *
   */
  private static class TimeWindowStatUpdater extends StatUpdater{

    final int collectBuckets;
    final int updatesPerBucket;
    
    private int updates;
    private int buckets;

    TimeWindowStatUpdater(TimeWindow w, int updatePeriod) {
      if (updatePeriod > w.updateGranularity) {
        throw new RuntimeException(
            "Invalid conf: updatePeriod > updateGranularity");
      }
      collectBuckets = w.windowSize / w.updateGranularity;
      updatesPerBucket = w.updateGranularity / updatePeriod;
    }

    synchronized void update() {
      updates++;
      if (updates == updatesPerBucket) {
        for(TimeStat stat : statToCollect.values()) {
          stat.addBucket();
        }
        updates = 0;
        buckets++;
        if (buckets > collectBuckets) {
          for (TimeStat stat : statToCollect.values()) {
            stat.removeBucket();
          }
          buckets--;
        }
      }
    }
  }

}
