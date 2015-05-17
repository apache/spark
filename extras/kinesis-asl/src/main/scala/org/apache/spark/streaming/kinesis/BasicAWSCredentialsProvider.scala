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
package org.apache.spark.streaming.kinesis

import org.apache.spark.annotation.DeveloperApi

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials

/**
 * :: DeveloperAPI ::
 * 
 * Implementation of AWSCredentialsProvider for BasicAWSCredentials.
 * (Surprisingly, this is not part of the AWS Java SDK.)
 * 
 * Note:  This is intentionally not Serializable to match the other AWSCredentials implementations.
 *        Making this Serializable could lead to a refactoring that would introduce a 
 *        NotSerializableExceptions when migrating to different AWSCredentials impls such as 
 *        DefaultAWSCredentialsProviderChain.
 *        In other words, I'm following the existing Non-Serializable hierarchy dictated by AWS.
 *
 * @param awsAccessKeyId  AWS Access Key Id
 * @param awsSecretKey  AWS Secret Key
 */
@DeveloperApi
class BasicAWSCredentialsProvider(awsAccessKeyId: String, awsSecretKey: String)
    extends AWSCredentialsProvider {
  
  override def getCredentials(): AWSCredentials =
    new BasicAWSCredentials(awsAccessKeyId, awsSecretKey)
  override def refresh() {}
  override def toString(): String = getClass().getSimpleName()
}
