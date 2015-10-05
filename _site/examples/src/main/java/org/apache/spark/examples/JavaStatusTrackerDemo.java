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

package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * Example of using Spark's status APIs from Java.
 */
public final class JavaStatusTrackerDemo {

  public static final String APP_NAME = "JavaStatusAPIDemo";

  public static final class IdentityWithDelay<T> implements Function<T, T> {
    @Override
    public T call(T x) throws Exception {
      Thread.sleep(2 * 1000);  // 2 seconds
      return x;
    }
  }

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
    final JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // Example of implementing a progress reporter for a simple job.
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(
        new IdentityWithDelay<Integer>());
    JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
    while (!jobFuture.isDone()) {
      Thread.sleep(1000);  // 1 second
      List<Integer> jobIds = jobFuture.jobIds();
      if (jobIds.isEmpty()) {
        continue;
      }
      int currentJobId = jobIds.get(jobIds.size() - 1);
      SparkJobInfo jobInfo = sc.statusTracker().getJobInfo(currentJobId);
      SparkStageInfo stageInfo = sc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
      System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
          " active, " + stageInfo.numCompletedTasks() + " complete");
    }

    System.out.println("Job results are: " + jobFuture.get());
    sc.stop();
  }
}
