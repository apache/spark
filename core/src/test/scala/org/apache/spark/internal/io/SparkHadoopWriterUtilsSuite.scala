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

package org.apache.spark.internal.io

import java.util.Date

import org.apache.hadoop.mapreduce.JobID

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.io.SparkHadoopWriterUtils.createJobID

/**
 * Unit tests for functions in SparkHadoopWriterUtils.
 */
class SparkHadoopWriterUtilsSuite extends SparkFunSuite {

  /**
   * Core test of JobID generation:
   * They are created.
   * The job number is converted to the job ID.
   * They round trip to string and back
   * (which implies that the full string matches the regexp
   * in the JobID class).
   */
  test("JobID Generation") {
    val jobNumber = 1010
    val j1 = createJobID(new Date(), jobNumber)
    assert(jobNumber == j1.getId,
      s"Job number mismatch in $j1")

    val jobStr = j1.toString
    // the string value begins with job_
    assert(jobStr.startsWith("job_"),
      s"wrong prefix of $jobStr")
    // and the hadoop code can parse it
    val j2 = roundTrip(j1)
    assert(j1.getId == j2.getId, "Job ID mismatch")
    assert(j1.getJtIdentifier == j2.getJtIdentifier, "Job identifier mismatch")
  }

  /**
   * This is the problem surfacing in situations where committers expect
   * Job IDs to be unique: if the timestamp is (exclusively) used
   * then there will conflict in directories created.
   */
  test("JobIDs generated at same time are different") {
    val now = new Date()
    val j1 = createJobID(now, 1)
    val j2 = createJobID(now, 1)
    assert(j1.toString != j2.toString)
  }

  /**
   * There's nothing explicitly in the Hadoop classes to stop
   * job numbers being negative.
   * There's some big assumptions in the FileOutputCommitter about attempt IDs
   * being positive during any recovery operations; for safety the ID
   * job number is validated.
   */
  test("JobIDs with negative job number") {
    intercept[IllegalArgumentException] {
      createJobID(new Date(), -1)
    }
  }

  /**
   * If someone ever does reinstate use of timestamps,
   * make sure that the case of timestamp == 0 is handled.
   */
  test("JobIDs on Epoch are different") {
    val j1 = createJobID(new Date(0), 0)
    val j2 = createJobID(new Date(0), 0)
    assert (j1.toString != j2.toString)
  }

  /**
   * Do a round trip as a string and back again.
   * This uses the JobID parser.
   * @param jobID job ID
   * @return the returned jobID
   */
  private def roundTrip(jobID: JobID): JobID = {
    val parsedJobId = JobID.forName(jobID.toString)
    assert(jobID == parsedJobId, "Round trip was inconsistent")
    parsedJobId
  }
}
