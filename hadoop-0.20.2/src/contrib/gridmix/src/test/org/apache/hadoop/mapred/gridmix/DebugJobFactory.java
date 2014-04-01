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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobStory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Component generating random job traces for testing on a single node.
 */
class DebugJobFactory {

  interface Debuggable {
    ArrayList<JobStory> getSubmitted();
  }

  public static JobFactory getFactory(
    JobSubmitter submitter, Path scratch, int numJobs, Configuration conf,
    CountDownLatch startFlag,UserResolver resolver) throws IOException {
    GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.getPolicy(
      conf, GridmixJobSubmissionPolicy.STRESS);
    if (policy==GridmixJobSubmissionPolicy.REPLAY) {
      return new DebugReplayJobFactory(
        submitter, scratch, numJobs, conf, startFlag,resolver);
    } else if (policy==GridmixJobSubmissionPolicy.STRESS) {
      return new DebugStressJobFactory(
        submitter, scratch, numJobs, conf, startFlag,resolver);
    } else if (policy==GridmixJobSubmissionPolicy.SERIAL) {
      return new DebugSerialJobFactory(
        submitter, scratch, numJobs, conf, startFlag,resolver);

    }
    return null;
  }

  static class DebugReplayJobFactory extends ReplayJobFactory
    implements Debuggable {
    public DebugReplayJobFactory(
      JobSubmitter submitter, Path scratch, int numJobs, Configuration conf,
      CountDownLatch startFlag,UserResolver resolver) throws IOException {
      super(
        submitter, new DebugJobProducer(numJobs, conf), scratch, conf,
        startFlag,resolver);
    }

    @Override
    public ArrayList<JobStory> getSubmitted() {
      return ((DebugJobProducer) jobProducer).submitted;
    }

  }

  static class DebugSerialJobFactory extends SerialJobFactory
    implements Debuggable {
    public DebugSerialJobFactory(
      JobSubmitter submitter, Path scratch, int numJobs, Configuration conf,
      CountDownLatch startFlag,UserResolver resolver) throws IOException {
      super(
        submitter, new DebugJobProducer(numJobs, conf), scratch, conf,
        startFlag,resolver);
    }

    @Override
    public ArrayList<JobStory> getSubmitted() {
      return ((DebugJobProducer) jobProducer).submitted;
    }

  }

  static class DebugStressJobFactory extends StressJobFactory
    implements Debuggable {
    public DebugStressJobFactory(
      JobSubmitter submitter, Path scratch, int numJobs, Configuration conf,
      CountDownLatch startFlag,UserResolver resolver) throws IOException {
      super(
        submitter, new DebugJobProducer(numJobs, conf), scratch, conf,
        startFlag,resolver);
    }

    @Override
    public ArrayList<JobStory> getSubmitted() {
      return ((DebugJobProducer) jobProducer).submitted;
    }
  }

}
