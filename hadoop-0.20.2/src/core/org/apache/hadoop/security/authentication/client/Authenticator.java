/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.client;


import java.io.IOException;
import java.net.URL;

/**
 * Interface for client authentication mechanisms.
 * <p/>
 * Implementations are use-once instances, they don't need to be thread safe.
 */
public interface Authenticator {

  /**
   * Authenticates against a URL and returns a {@link AuthenticatedURL.Token} to be
   * used by subsequent requests.
   *
   * @param url the URl to authenticate against.
   * @param token the authentication token being used for the user.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication error occurred.
   */
  public void authenticate(URL url, AuthenticatedURL.Token token) throws IOException, AuthenticationException;

}
