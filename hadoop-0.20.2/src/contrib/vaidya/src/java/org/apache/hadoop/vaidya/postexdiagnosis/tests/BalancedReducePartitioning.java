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
public class BalancedReducePartitioning extends DiagnosticTest {

  private long totalReduces;
  private long busyReducers;
  private long percentReduceRecordsSize;
  private double percent;
  private double impact;
  private JobStatistics _job;
  
  /**
   * 
   */
  public BalancedReducePartitioning() {
  }

  /*    
   */
  @Override
  public double evaluate(JobStatistics jobExecutionStats) {
    
    /* Set the global job variable */
    this._job = jobExecutionStats;

    /* If Map only job then impact is zero */
    if (jobExecutionStats.getStringValue(JobKeys.JOBTYPE).equals("MAP_ONLY")) {
      this.impact = 0;
      return this.impact;
    }

    /*
     * Read this rule specific input PercentReduceRecords
     */
    this.percent = getInputElementDoubleValue("PercentReduceRecords", 0.90);
    
    /*
     * Get the sorted reduce task list by number of INPUT_RECORDS (ascending) 
     */
    List<ReduceTaskStatistics> srTaskList = 
                            jobExecutionStats.getReduceTaskList(ReduceTaskKeys.INPUT_RECORDS, KeyDataType.LONG);
    this.percentReduceRecordsSize = (long) (this.percent * jobExecutionStats.getLongValue(JobKeys.REDUCE_INPUT_RECORDS));
    this.totalReduces = jobExecutionStats.getLongValue(JobKeys.TOTAL_REDUCES);
    long tempReduceRecordsCount = 0;
    this.busyReducers = 0;
    for (int i=srTaskList.size()-1; i>-1; i--) {
      tempReduceRecordsCount += srTaskList.get(i).getLongValue(ReduceTaskKeys.INPUT_RECORDS);
      this.busyReducers++;
      if (tempReduceRecordsCount >= this.percentReduceRecordsSize) {
        break;
      }
    }
    
    // Calculate Impact
    return this.impact = (1 - (double)this.busyReducers/(double)this.totalReduces);
  }

  /*
   * helper function to print specific reduce counter for all reduce tasks
   */
  public void printReduceCounters (List<Hashtable<ReduceTaskKeys, String>> x, ReduceTaskKeys key) {
    for (int i=0; i<x.size(); i++) {
      System.out.println("ind:"+i+", Value:"+x.get(i).get(key)+":");
    }
  }
  
  /* 
   * 
   */
  @Override
  public String getPrescription() {
    return 
    "* Use the appropriate partitioning function"+ "\n" +
    "* For streaming job consider following partitioner and hadoop config parameters\n"+
    "  * org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner\n" +
    "  * -jobconf stream.map.output.field.separator, -jobconf stream.num.map.output.key.fields";
  }

  /* 
   */
  @Override
  public String getReferenceDetails() {
    String ref = 
    "* TotalReduceTasks: "+this.totalReduces+"\n"+
    "* BusyReduceTasks processing "+this.percent+ "% of total records: " +this.busyReducers+"\n"+
    "* Impact: "+truncate(this.impact);
    return ref;
  }
}
