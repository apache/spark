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

package org.apache.spark.sql.sqs

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.util.Utils

/**
 * User specified options for sqs source.
 */
class SqsSourceOptions(parameters: CaseInsensitiveMap[String]) extends Logging {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val maxFilesPerTrigger: Option[Int] = parameters.get("maxFilesPerTrigger").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'maxFilesPerTrigger', must be a positive integer")
    }
  }

  /**
   * Maximum age of a file that can be found in this directory, before it is ignored. For the
   * first batch all files will be considered valid.
   *
   * The max age is specified with respect to the timestamp of the latest file, and not the
   * timestamp of the current system. That this means if the last file has timestamp 1000, and the
   * current system time is 2000, and max age is 200, the system will purge files older than
   * 800 (rather than 1800) from the internal state.
   *
   * Default to a week.
   */
  val maxFileAgeMs: Long =
    Utils.timeStringAsMs(parameters.getOrElse("maxFileAge", "7d"))

  val fetchIntervalSeconds: Int = parameters.get("sqsFetchIntervalSeconds").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'sqsFetchIntervalSeconds', must be a positive integer")
    }
  }.getOrElse(10)

  val longPollWaitTimeSeconds: Int = parameters.get("sqsLongPollingWaitTimeSeconds").map { str =>
    Try(str.toInt).toOption.filter(x => x >= 0 && x <= 20).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'sqsLongPollingWaitTimeSeconds'," +
          "must be an integer between 0 and 20")
    }
  }.getOrElse(20)

  val maxRetries: Int = parameters.get("sqsMaxRetries").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'sqsMaxRetries', must be a positive integer")
    }
  }.getOrElse(10)

  val maxConnections: Int = parameters.get("sqsMaxConnections").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'sqsMaxConnections', must be a positive integer")
    }
  }.getOrElse(1)

  val sqsUrl: String = parameters.get("sqsUrl").getOrElse{
    throw new IllegalArgumentException("SQS Url is not specified")
  }

  val region: String = parameters.get("region").getOrElse {
    throw new IllegalArgumentException("Region is not specified")
  }

  val fileFormatClassName: String = parameters.get("fileFormat").getOrElse {
    throw new IllegalArgumentException("Specifying file format is mandatory with sqs source")
  }

  val ignoreFileDeletion: Boolean = withBooleanParameter("ignoreFileDeletion", false)

  /**
    * Whether to check new files based on only the filename instead of on the full path.
    *
    * With this set to `true`, the following files would be considered as the same file, because
    * their filenames, "dataset.txt", are the same:
    * - "file:///dataset.txt"
    * - "s3://a/dataset.txt"
    * - "s3n://a/b/dataset.txt"
    * - "s3a://a/b/c/dataset.txt"
    */
  val fileNameOnly: Boolean = withBooleanParameter("fileNameOnly", false)

  val shouldSortFiles: Boolean = withBooleanParameter("shouldSortFiles", true)

  val useInstanceProfileCredentials: Boolean = withBooleanParameter(
    "useInstanceProfileCredentials", false)

  private def withBooleanParameter(name: String, default: Boolean) = {
    parameters.get(name).map { str =>
      try {
        str.toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option '$name', must be true or false")
      }
    }.getOrElse(default)
  }

}
