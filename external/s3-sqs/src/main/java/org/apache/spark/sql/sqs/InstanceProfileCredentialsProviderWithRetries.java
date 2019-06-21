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
 
 package org.apache.spark.sql.sqs;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InstanceProfileCredentialsProviderWithRetries extends InstanceProfileCredentialsProvider {

    private static final Log LOG = LogFactory.getLog(InstanceProfileCredentialsProviderWithRetries.class);

    public AWSCredentials getCredentials() {
        int retries = 10;
        int sleep = 500;
        while(retries > 0) {
            try {
                return super.getCredentials();
            }
            catch (RuntimeException re) {
                LOG.error("Got an exception while fetching credentials " + re);
                --retries;
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                }
                if (sleep < 10000) {
                    sleep *= 2;
                }
            }
            catch (Error error) {
                LOG.error("Got an exception while fetching credentials " + error);
                --retries;
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                }
                if (sleep < 10000) {
                    sleep *= 2;
                }
            }
        }
        throw new AmazonClientException("Unable to load credentials.");
    }
}
