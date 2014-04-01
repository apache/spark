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
public class ReadingHDFSFilesAsSideEffect extends DiagnosticTest {

  private double _impact;
  private JobStatistics _job;
  
  /**
   * 
   */
  public ReadingHDFSFilesAsSideEffect() {
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
     * Read the Normalization Factor
     */
    double normF = getInputElementDoubleValue("NormalizationFactor", 2.0);
    
    
    /*
     * Calculate and return the impact
     * 
     * Check if job level aggregate bytes read from HDFS are more than map input bytes
     * Typically they should be same unless maps and/or reducers are reading some data
     * from HDFS as a side effect
     * 
     * If side effect HDFS bytes read are >= twice map input bytes impact is treated as
     * maximum.
     */
    if(job.getLongValue(JobKeys.MAP_INPUT_BYTES) == 0 && job.getLongValue(JobKeys.HDFS_BYTES_READ) != 0) {
      return (double)1;
    }

    if (job.getLongValue(JobKeys.HDFS_BYTES_READ) == 0) {
      return (double)0;
    }
    
    this._impact = (job.getLongValue(JobKeys.HDFS_BYTES_READ) / job.getLongValue(JobKeys.MAP_INPUT_BYTES));
    if (this._impact >= normF) {
      this._impact = 1;
    }
    else  {
      this._impact = this._impact/normF;
    }
    
    return this._impact;
  }

  
  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.utils.perfadvisor.diagnostic_rules.DiagnosticRule#getAdvice()
   */
  @Override
  public String getPrescription() {
    return 
    "Map and/or Reduce tasks are reading application specific files from HDFS. Make sure the replication factor\n" +
        "of these HDFS files is high enough to avoid the data reading bottleneck. Typically replication factor\n" +
        "can be square root of map/reduce tasks capacity of the allocated cluster.";
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.utils.perfadvisor.diagnostic_rules.DiagnosticRule#getReferenceDetails()
   */
  @Override
  public String getReferenceDetails() {
    String ref = "* Total HDFS Bytes read: "+this._job.getLongValue(JobKeys.HDFS_BYTES_READ)+"\n"+
                 "* Total Map Input Bytes read: "+this._job.getLongValue(JobKeys.MAP_INPUT_BYTES)+"\n"+
                 "* Impact: "+truncate(this._impact);
    return ref;
  }
}
