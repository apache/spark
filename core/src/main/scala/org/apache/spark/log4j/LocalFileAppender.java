/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.log4j;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.spark.SparkEnv;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class LocalFileAppender extends FileAppender {

  private String metadataIdentifier;
  private String category;
  private String identifier;
  private String jobName;
  private String project;
  private String executorId;
  private String mountDir;

  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  public LocalFileAppender() {
    super();
  }

  public LocalFileAppender(Layout layout, String filename, boolean append) throws IOException {
    super(layout, filename, append);
  }

  public LocalFileAppender(Layout layout, String filename) throws IOException {
    super(layout, filename);
  }

  @Override
  public void activateOptions() {
    LogLog.warn(String.format("%s starting ...", name));
    this.fileName = getOutPutDir();
    LogLog.warn(String.format("Output path is %s", this.fileName));
    LogLog.warn("metadataIdentifier -> " + getMetadataIdentifier());
    LogLog.warn("category -> " + getCategory());
    LogLog.warn("identifier -> " + getIdentifier());
    LogLog.warn("mountDir -> " + getMountDir());

    if (null != getProject()) {
      LogLog.warn("project -> " + getProject());
    }

    if (null != getJobName()) {
      LogLog.warn("jobName -> " + getJobName());
    }
    super.activateOptions();
  }

  @VisibleForTesting
  String getOutPutDir() {
    String rollingDir = dateFormat.format(new Date());
    if (StringUtils.isBlank(executorId)) {
      executorId =
          SparkEnv.get() != null ? SparkEnv.get().executorId() : UUID.randomUUID().toString();
      LogLog.warn("executorId set to " + executorId);
    }

    if ("job".equals(getCategory())) {
      return getRootPathName() + "/" + rollingDir +
          "/" + getIdentifier() + "/" + getJobName() + "/" + "executor-"
          + executorId + ".log";
    }
    return getRootPathName() + "/" + rollingDir
        + "/" + getIdentifier() + "/" + "executor-" + executorId + ".log";
  }

  String getRootPathName() {
    if (!mountDir.endsWith("/")) {
      mountDir = mountDir + "/";
    }
    if ("job".equals(getCategory())) {
      return mountDir + getProject() + "/spark_logs";
    } else if ("sparder".equals(getCategory())) {
      return mountDir + "_sparder_logs";
    } else {
      throw new IllegalArgumentException("illegal category: " + getCategory());
    }
  }


  public String getMetadataIdentifier() {
    return metadataIdentifier;
  }

  public void setMetadataIdentifier(String metadataIdentifier) {
    this.metadataIdentifier = metadataIdentifier;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getExecutorId() {
    return executorId;
  }

  public void setExecutorId(String executorId) {
    this.executorId = executorId;
  }

  public String getMountDir() {
    return mountDir;
  }

  public void setMountDir(String mountDir) {
    this.mountDir = mountDir;
  }

}