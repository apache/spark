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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Utility class for skip bad records functionality. It contains various 
 * settings related to skipping of bad records.
 * 
 * <p>Hadoop provides an optional mode of execution in which the bad records
 * are detected and skipped in further attempts.
 * 
 * <p>This feature can be used when map/reduce tasks crashes deterministically on 
 * certain input. This happens due to bugs in the map/reduce function. The usual
 * course would be to fix these bugs. But sometimes this is not possible; 
 * perhaps the bug is in third party libraries for which the source code is 
 * not available. Due to this, the task never reaches to completion even with 
 * multiple attempts and complete data for that task is lost.</p>
 *  
 * <p>With this feature, only a small portion of data is lost surrounding 
 * the bad record, which may be acceptable for some user applications.
 * see {@link SkipBadRecords#setMapperMaxSkipRecords(Configuration, long)}</p>
 * 
 * <p>The skipping mode gets kicked off after certain no of failures 
 * see {@link SkipBadRecords#setAttemptsToStartSkipping(Configuration, int)}</p>
 *  
 * <p>In the skipping mode, the map/reduce task maintains the record range which 
 * is getting processed at all times. Before giving the input to the
 * map/reduce function, it sends this record range to the Task tracker.
 * If task crashes, the Task tracker knows which one was the last reported
 * range. On further attempts that range get skipped.</p>
 */
public class SkipBadRecords {
  
  /**
   * Special counters which are written by the application and are 
   * used by the framework for detecting bad records. For detecting bad records 
   * these counters must be incremented by the application.
   */
  public static final String COUNTER_GROUP = "SkippingTaskCounters";
  
  /**
   * Number of processed map records.
   * @see SkipBadRecords#getAutoIncrMapperProcCount(Configuration)
   */
  public static final String COUNTER_MAP_PROCESSED_RECORDS = 
    "MapProcessedRecords";
  
  /**
   * Number of processed reduce groups.
   * @see SkipBadRecords#getAutoIncrReducerProcCount(Configuration)
   */
  public static final String COUNTER_REDUCE_PROCESSED_GROUPS = 
    "ReduceProcessedGroups";
  
  private static final String ATTEMPTS_TO_START_SKIPPING = 
    "mapred.skip.attempts.to.start.skipping";
  private static final String AUTO_INCR_MAP_PROC_COUNT = 
    "mapred.skip.map.auto.incr.proc.count";
  private static final String AUTO_INCR_REDUCE_PROC_COUNT = 
    "mapred.skip.reduce.auto.incr.proc.count";
  private static final String OUT_PATH = "mapred.skip.out.dir";
  private static final String MAPPER_MAX_SKIP_RECORDS = 
    "mapred.skip.map.max.skip.records";
  private static final String REDUCER_MAX_SKIP_GROUPS = 
    "mapred.skip.reduce.max.skip.groups";
  
  /**
   * Get the number of Task attempts AFTER which skip mode 
   * will be kicked off. When skip mode is kicked off, the 
   * tasks reports the range of records which it will process 
   * next to the TaskTracker. So that on failures, TT knows which 
   * ones are possibly the bad records. On further executions, 
   * those are skipped.
   * Default value is 2.
   * 
   * @param conf the configuration
   * @return attemptsToStartSkipping no of task attempts
   */
  public static int getAttemptsToStartSkipping(Configuration conf) {
    return conf.getInt(ATTEMPTS_TO_START_SKIPPING, 2);
  }

  /**
   * Set the number of Task attempts AFTER which skip mode 
   * will be kicked off. When skip mode is kicked off, the 
   * tasks reports the range of records which it will process 
   * next to the TaskTracker. So that on failures, TT knows which 
   * ones are possibly the bad records. On further executions, 
   * those are skipped.
   * Default value is 2.
   * 
   * @param conf the configuration
   * @param attemptsToStartSkipping no of task attempts
   */
  public static void setAttemptsToStartSkipping(Configuration conf, 
      int attemptsToStartSkipping) {
    conf.setInt(ATTEMPTS_TO_START_SKIPPING, attemptsToStartSkipping);
  }

  /**
   * Get the flag which if set to true, 
   * {@link SkipBadRecords#COUNTER_MAP_PROCESSED_RECORDS} is incremented 
   * by MapRunner after invoking the map function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * Default value is true.
   * 
   * @param conf the configuration
   * @return <code>true</code> if auto increment 
   *                       {@link SkipBadRecords#COUNTER_MAP_PROCESSED_RECORDS}.
   *         <code>false</code> otherwise.
   */
  public static boolean getAutoIncrMapperProcCount(Configuration conf) {
    return conf.getBoolean(AUTO_INCR_MAP_PROC_COUNT, true);
  }
  
  /**
   * Set the flag which if set to true, 
   * {@link SkipBadRecords#COUNTER_MAP_PROCESSED_RECORDS} is incremented 
   * by MapRunner after invoking the map function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * Default value is true.
   * 
   * @param conf the configuration
   * @param autoIncr whether to auto increment 
   *        {@link SkipBadRecords#COUNTER_MAP_PROCESSED_RECORDS}.
   */
  public static void setAutoIncrMapperProcCount(Configuration conf, 
      boolean autoIncr) {
    conf.setBoolean(AUTO_INCR_MAP_PROC_COUNT, autoIncr);
  }
  
  /**
   * Get the flag which if set to true, 
   * {@link SkipBadRecords#COUNTER_REDUCE_PROCESSED_GROUPS} is incremented 
   * by framework after invoking the reduce function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * Default value is true.
   * 
   * @param conf the configuration
   * @return <code>true</code> if auto increment 
   *                    {@link SkipBadRecords#COUNTER_REDUCE_PROCESSED_GROUPS}.
   *         <code>false</code> otherwise.
   */
  public static boolean getAutoIncrReducerProcCount(Configuration conf) {
    return conf.getBoolean(AUTO_INCR_REDUCE_PROC_COUNT, true);
  }
  
  /**
   * Set the flag which if set to true, 
   * {@link SkipBadRecords#COUNTER_REDUCE_PROCESSED_GROUPS} is incremented 
   * by framework after invoking the reduce function. This value must be set to 
   * false for applications which process the records asynchronously 
   * or buffer the input records. For example streaming. 
   * In such cases applications should increment this counter on their own.
   * Default value is true.
   * 
   * @param conf the configuration
   * @param autoIncr whether to auto increment 
   *        {@link SkipBadRecords#COUNTER_REDUCE_PROCESSED_GROUPS}.
   */
  public static void setAutoIncrReducerProcCount(Configuration conf, 
      boolean autoIncr) {
    conf.setBoolean(AUTO_INCR_REDUCE_PROC_COUNT, autoIncr);
  }
  
  /**
   * Get the directory to which skipped records are written. By default it is 
   * the sub directory of the output _logs directory.
   * User can stop writing skipped records by setting the value null.
   * 
   * @param conf the configuration.
   * @return path skip output directory. Null is returned if this is not set 
   * and output directory is also not set.
   */
  public static Path getSkipOutputPath(Configuration conf) {
    String name =  conf.get(OUT_PATH);
    if(name!=null) {
      if("none".equals(name)) {
        return null;
      }
      return new Path(name);
    }
    Path outPath = FileOutputFormat.getOutputPath(new JobConf(conf));
    return outPath==null ? null : new Path(outPath, 
        "_logs"+Path.SEPARATOR+"skip");
  }
  
  /**
   * Set the directory to which skipped records are written. By default it is 
   * the sub directory of the output _logs directory.
   * User can stop writing skipped records by setting the value null.
   * 
   * @param conf the configuration.
   * @param path skip output directory path
   */
  public static void setSkipOutputPath(JobConf conf, Path path) {
    String pathStr = null;
    if(path==null) {
      pathStr = "none";
    } else {
      pathStr = path.toString();
    }
    conf.set(OUT_PATH, pathStr);
  }
  
  /**
   * Get the number of acceptable skip records surrounding the bad record PER 
   * bad record in mapper. The number includes the bad record as well.
   * To turn the feature of detection/skipping of bad records off, set the 
   * value to 0.
   * The framework tries to narrow down the skipped range by retrying  
   * until this threshold is met OR all attempts get exhausted for this task. 
   * Set the value to Long.MAX_VALUE to indicate that framework need not try to 
   * narrow down. Whatever records(depends on application) get skipped are 
   * acceptable.
   * Default value is 0.
   * 
   * @param conf the configuration
   * @return maxSkipRecs acceptable skip records.
   */
  public static long getMapperMaxSkipRecords(Configuration conf) {
    return conf.getLong(MAPPER_MAX_SKIP_RECORDS, 0);
  }
  
  /**
   * Set the number of acceptable skip records surrounding the bad record PER 
   * bad record in mapper. The number includes the bad record as well.
   * To turn the feature of detection/skipping of bad records off, set the 
   * value to 0.
   * The framework tries to narrow down the skipped range by retrying  
   * until this threshold is met OR all attempts get exhausted for this task. 
   * Set the value to Long.MAX_VALUE to indicate that framework need not try to 
   * narrow down. Whatever records(depends on application) get skipped are 
   * acceptable.
   * Default value is 0.
   * 
   * @param conf the configuration
   * @param maxSkipRecs acceptable skip records.
   */
  public static void setMapperMaxSkipRecords(Configuration conf, 
      long maxSkipRecs) {
    conf.setLong(MAPPER_MAX_SKIP_RECORDS, maxSkipRecs);
  }
  
  /**
   * Get the number of acceptable skip groups surrounding the bad group PER 
   * bad group in reducer. The number includes the bad group as well.
   * To turn the feature of detection/skipping of bad groups off, set the 
   * value to 0.
   * The framework tries to narrow down the skipped range by retrying  
   * until this threshold is met OR all attempts get exhausted for this task. 
   * Set the value to Long.MAX_VALUE to indicate that framework need not try to 
   * narrow down. Whatever groups(depends on application) get skipped are 
   * acceptable.
   * Default value is 0.
   * 
   * @param conf the configuration
   * @return maxSkipGrps acceptable skip groups.
   */
  public static long getReducerMaxSkipGroups(Configuration conf) {
    return conf.getLong(REDUCER_MAX_SKIP_GROUPS, 0);
  }
  
  /**
   * Set the number of acceptable skip groups surrounding the bad group PER 
   * bad group in reducer. The number includes the bad group as well.
   * To turn the feature of detection/skipping of bad groups off, set the 
   * value to 0.
   * The framework tries to narrow down the skipped range by retrying  
   * until this threshold is met OR all attempts get exhausted for this task. 
   * Set the value to Long.MAX_VALUE to indicate that framework need not try to 
   * narrow down. Whatever groups(depends on application) get skipped are 
   * acceptable.
   * Default value is 0.
   * 
   * @param conf the configuration
   * @param maxSkipGrps acceptable skip groups.
   */
  public static void setReducerMaxSkipGroups(Configuration conf, 
      long maxSkipGrps) {
    conf.setLong(REDUCER_MAX_SKIP_GROUPS, maxSkipGrps);
  }
}
