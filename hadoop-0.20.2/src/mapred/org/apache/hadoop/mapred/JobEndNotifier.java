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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JobEndNotifier {
  private static final Log LOG =
    LogFactory.getLog(JobEndNotifier.class.getName());

  private static Thread thread;
  private static volatile boolean running;
  private static BlockingQueue<JobEndStatusInfo> queue =
    new DelayQueue<JobEndStatusInfo>();

  public static void startNotifier() {
    running = true;
    thread = new Thread(
                        new Runnable() {
                          public void run() {
                            try {
                              while (running) {
                                sendNotification(queue.take());
                              }
                            }
                            catch (InterruptedException irex) {
                              if (running) {
                                LOG.error("Thread has ended unexpectedly", irex);
                              }
                            }
                          }

                          private void sendNotification(JobEndStatusInfo notification) {
                            try {
                              int code = httpNotification(notification.getUri());
                              if (code != 200) {
                                throw new IOException("Invalid response status code: " + code);
                              }
                            }
                            catch (IOException ioex) {
                              LOG.error("Notification failure [" + notification + "]", ioex);
                              if (notification.configureForRetry()) {
                                try {
                                  queue.put(notification);
                                }
                                catch (InterruptedException iex) {
                                  LOG.error("Notification queuing error [" + notification + "]",
                                            iex);
                                }
                              }
                            }
                            catch (Exception ex) {
                              LOG.error("Notification failure [" + notification + "]", ex);
                            }
                          }

                        }

                        );
    thread.start();
  }

  public static void stopNotifier() {
    running = false;
    thread.interrupt();
  }

  private static JobEndStatusInfo createNotification(JobConf conf,
                                                     JobStatus status) {
    JobEndStatusInfo notification = null;
    String uri = conf.getJobEndNotificationURI();
    if (uri != null) {
      // +1 to make logic for first notification identical to a retry
      int retryAttempts = conf.getInt("job.end.retry.attempts", 0) + 1;
      long retryInterval = conf.getInt("job.end.retry.interval", 30000);
      if (uri.contains("$jobId")) {
        uri = uri.replace("$jobId", status.getJobID().toString());
      }
      if (uri.contains("$jobStatus")) {
        String statusStr =
          (status.getRunState() == JobStatus.SUCCEEDED) ? "SUCCEEDED" : 
            (status.getRunState() == JobStatus.FAILED) ? "FAILED" : "KILLED";
        uri = uri.replace("$jobStatus", statusStr);
      }
      notification = new JobEndStatusInfo(uri, retryAttempts, retryInterval);
    }
    return notification;
  }

  public static void registerNotification(JobConf jobConf, JobStatus status) {
    JobEndStatusInfo notification = createNotification(jobConf, status);
    if (notification != null) {
      try {
        queue.put(notification);
      }
      catch (InterruptedException iex) {
        LOG.error("Notification queuing failure [" + notification + "]", iex);
      }
    }
  }

  private static int httpNotification(String uri) throws IOException {
    URI url = new URI(uri, false);
    HttpClient m_client = new HttpClient();
    HttpMethod method = new GetMethod(url.getEscapedURI());
    method.setRequestHeader("Accept", "*/*");
    return m_client.executeMethod(method);
  }

  // for use by the LocalJobRunner, without using a thread&queue,
  // simple synchronous way
  public static void localRunnerNotification(JobConf conf, JobStatus status) {
    JobEndStatusInfo notification = createNotification(conf, status);
    if (notification != null) {
      while (notification.configureForRetry()) {
        try {
          int code = httpNotification(notification.getUri());
          if (code != 200) {
            throw new IOException("Invalid response status code: " + code);
          }
          else {
            break;
          }
        }
        catch (IOException ioex) {
          LOG.error("Notification error [" + notification.getUri() + "]", ioex);
        }
        catch (Exception ex) {
          LOG.error("Notification error [" + notification.getUri() + "]", ex);
        }
        try {
          synchronized (Thread.currentThread()) {
            Thread.currentThread().sleep(notification.getRetryInterval());
          }
        }
        catch (InterruptedException iex) {
          LOG.error("Notification retry error [" + notification + "]", iex);
        }
      }
    }
  }

  private static class JobEndStatusInfo implements Delayed {
    private String uri;
    private int retryAttempts;
    private long retryInterval;
    private long delayTime;

    JobEndStatusInfo(String uri, int retryAttempts, long retryInterval) {
      this.uri = uri;
      this.retryAttempts = retryAttempts;
      this.retryInterval = retryInterval;
      this.delayTime = System.currentTimeMillis();
    }

    public String getUri() {
      return uri;
    }

    public int getRetryAttempts() {
      return retryAttempts;
    }

    public long getRetryInterval() {
      return retryInterval;
    }

    public long getDelayTime() {
      return delayTime;
    }

    public boolean configureForRetry() {
      boolean retry = false;
      if (getRetryAttempts() > 0) {
        retry = true;
        delayTime = System.currentTimeMillis() + retryInterval;
      }
      retryAttempts--;
      return retry;
    }

    public long getDelay(TimeUnit unit) {
      long n = this.delayTime - System.currentTimeMillis();
      return unit.convert(n, TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed d) {
      return (int)(delayTime - ((JobEndStatusInfo)d).delayTime);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof JobEndStatusInfo)) {
        return false;
      }
      if (delayTime == ((JobEndStatusInfo)o).delayTime) {
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 37 * 17 + (int) (delayTime^(delayTime>>>32));
    }
      
    @Override
    public String toString() {
      return "URL: " + uri + " remaining retries: " + retryAttempts +
        " interval: " + retryInterval;
    }

  }

}
