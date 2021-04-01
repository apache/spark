/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.util;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HttpUtils {

  public static String getUrl(String url) {
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(url);
      try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
        if (response.getStatusLine().getStatusCode() != 200) {
          throw new RssException(String.format(
              "Failed to get url %s, response %s, %s",
              url, response.getStatusLine().getStatusCode(),
              IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8)));
        }
        return IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to get url " + url, e);
    }
  }

}
