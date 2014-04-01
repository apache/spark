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
package org.apache.hadoop.mapred.gridmix;
/**
 * Gridmix run modes. 
 *
 */
public class GridMixRunMode {
   public static final int DATA_GENERATION = 1;
   public static final int RUN_GRIDMIX = 2;
   public static final int DATA_GENERATION_AND_RUN_GRIDMIX = 3;
   private static String [] modeStr = {"DATA GENERATION",
      "RUNNING GRIDMIX",
      "DATA GENERATION AND RUNNING GRIDMIX"};
   /**
    * Get the appropriate message against the mode.
    * @param mode - grimdix run mode either 1 or 2 or 3.
    * @return - message as string.
    */
   public static String getMode(int mode){
     return modeStr[mode-1];
   }
}
