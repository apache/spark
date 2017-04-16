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

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

/** 
 * This exception is thrown when modification to HDFS results in violation
 * of a directory quota. A directory quota might be namespace quota (limit 
 * on number of files and directories) or a diskspace quota (limit on space 
 * taken by all the file under the directory tree). <br> <br>
 * 
 * The message for the exception specifies the directory where the quota
 * was violated and actual quotas. Specific message is generated in the 
 * corresponding Exception class: 
 *  DSQuotaExceededException or
 *  NSQuotaExceededException
 */
public class QuotaExceededException extends IOException {
  protected static final long serialVersionUID = 1L;
  protected String pathName=null;
  protected long quota; // quota
  protected long count; // actual value
  
  protected QuotaExceededException(String msg) {
    super(msg);
  }
  
  protected QuotaExceededException(long quota, long count) {
    this.quota = quota;
    this.count = count;
  }
  
  public void setPathName(String path) {
    this.pathName = path;
  }
  
  public String getMessage() {
    return super.getMessage();
  }
}
