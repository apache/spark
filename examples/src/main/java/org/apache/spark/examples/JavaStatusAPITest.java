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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Example of using Spark's status APIs from Java.
 */
public final class JavaStatusAPITest {

  public static final String APP_NAME = "JavaStatusAPITest";

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

    // Example of implementing a simple progress reporter for a single-action job.
    // TODO: refactor this to use collectAsync() once we've implemented AsyncRDDActions in Java.

    // Submit a single-stage job asynchronously:
    ExecutorService pool = Executors.newFixedThreadPool(1);
    Future<List<Integer>> jobFuture = pool.submit(new Callable<List<Integer>>() {
      @Override
      public List<Integer> call() {
        try {
          sc.setJobGroup(APP_NAME, "[Job Group Description]");
          return sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(
              new IdentityWithDelay<Integer>()).collect();
        } catch (Exception e) {
          System.out.println("Got exception while submitting job: ");
          e.printStackTrace();
          throw e;
        }
      }
    });

    // Monitor the progress of our job
    while (sc.getJobsIdsForGroup(APP_NAME).length == 0) {
      System.out.println("Waiting for job to be submitted to scheduler");
      Thread.sleep(1000);
    }
    int jobId = sc.getJobsIdsForGroup(APP_NAME)[0];
    System.out.println("Job was submitted with id " + jobId);
    while (!jobFuture.isDone()) {
      Thread.sleep(1000);  // 1 second
      SparkJobInfo jobInfo = sc.getJobInfo(jobId);
      SparkStageInfo stageInfo = sc.getStageInfo(jobInfo.stageIds()[0]);
      System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
          " active, " + stageInfo.numCompleteTasks() + " complete");
    }

    System.out.println("Job results are: " + jobFuture.get());
    pool.shutdown();
    sc.stop();
  }
}
