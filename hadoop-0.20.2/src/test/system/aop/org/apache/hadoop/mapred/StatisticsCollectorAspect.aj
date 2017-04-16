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

import org.apache.hadoop.mapred.StatisticsCollector.TimeWindow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * This class will change the number of jobs time windows 
 * of all task trackers <br/> 
 * Last Day time window will be changed from 24 hours to 2 minutes <br/> 
 * Last Hour time window will be changed from 1 hour to 1 minute <br/>
 */

public privileged aspect StatisticsCollectorAspect {

  //last day is changed to 120 seconds instead of 24 hours, 
  //with a 10 seconds refresh rate 
  static final TimeWindow 
    LAST_DAY_ASPECT = new TimeWindow("Last Day", 2 * 60, 10);

  //last day is changed to 60 seconds instead of 1 hour, 
  //with a 10 seconds refresh rate 
  static final TimeWindow 
    LAST_HOUR_ASPECT = new TimeWindow("Last Hour", 60, 10);

  private static final Log LOG = LogFactory
      .getLog(StatisticsCollectorAspect.class);

  pointcut createStatExecution(String name, TimeWindow[] window) : 
     call(* StatisticsCollector.createStat(String, TimeWindow[])) 
    && args(name, window);

  //This will change the timewindow to have last day and last hour changed.
  before(String name, TimeWindow[] window) : createStatExecution(name, window) {
      window[1] = LAST_DAY_ASPECT;
      window[2] = LAST_HOUR_ASPECT;
  }

}
