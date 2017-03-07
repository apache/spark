/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.kubernetes.integrationtest;

/**
 * Primarily extracted so that a separate jar can be added as a dependency for the
 * test Spark job.
 */
public class PiHelper {
  public static int helpPi() {
    double x = Math.random() * 2 - 1;
    double y = Math.random() * 2 - 1;
    if (x*x + y*y < 1) {
      return 1;
    } else {
      return 0;
    }
  }
}
