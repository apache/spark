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
package org.apache.hadoop.vaidya.postexdiagnosis.tests;

import org.apache.hadoop.vaidya.statistics.job.JobStatistics;
import org.apache.hadoop.vaidya.statistics.job.JobStatisticsInterface.JobKeys;
import org.apache.hadoop.vaidya.statistics.job.JobStatisticsInterface.KeyDataType;
import org.apache.hadoop.vaidya.statistics.job.JobStatisticsInterface.ReduceTaskKeys;
import org.apache.hadoop.vaidya.statistics.job.ReduceTaskStatistics;
import org.apache.hadoop.vaidya.DiagnosticTest;
import org.w3c.dom.Element;
import java.util.Hashtable;
import java.util.List;

/**
 *
 */
public class MapsReExecutionImpact extends DiagnosticTest {

  private double _impact;
  private JobStatistics _job;
  private long _percentMapsReExecuted;
  
  /**
   * 
   */
  public MapsReExecutionImpact() {
  }

  /*
   * Evaluate the test    
   */
  @Override
  public double evaluate(JobStatistics job) {
    
    /*
     * Set the this._job
     */
    this._job = job;

    /*
     * Calculate and return the impact
     */
    this._impact = ((job.getLongValue(JobKeys.LAUNCHED_MAPS) - job.getLongValue(JobKeys.TOTAL_MAPS))/job.getLongValue(JobKeys.TOTAL_MAPS));
    this._percentMapsReExecuted = Math.round(this._impact * 100);
    return this._impact;
  }

  
  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.utils.perfadvisor.diagnostic_rules.DiagnosticRule#getAdvice()
   */
  @Override
  public String getPrescription() {
    return 
    "* Need careful evaluation of why maps are re-executed. \n" +
      "  * It could be due to some set of unstable cluster nodes.\n" +
      "  * It could be due application specific failures.";
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.utils.perfadvisor.diagnostic_rules.DiagnosticRule#getReferenceDetails()
   */
  @Override
  public String getReferenceDetails() {
    String ref = "* Total Map Tasks: "+this._job.getLongValue(JobKeys.TOTAL_MAPS)+"\n"+
                 "* Launched Map Tasks: "+this._job.getLongValue(JobKeys.LAUNCHED_MAPS)+"\n"+
                 "* Percent Maps ReExecuted: "+this._percentMapsReExecuted+"\n"+
                 "* Impact: "+truncate(this._impact);
    return ref;
  }
}
